"""运行历史断尾回执回归测试：损坏尾行不中断读取，新旧批次按来源各结算一次。"""
from __future__ import annotations

import json
import os
import sys

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist
from app.services.batch_queue import BatchQueueService
from app.services.run_history import RunHistoryService
from app.store import store

RUN_LOG = "run_history.jsonl"


def _reset_runs_dir() -> None:
    for name in persist.list_files("runs"):
        os.remove(persist.data_path("runs", name))


def _reset_store() -> None:
    """恢复种子数据，让每个用例从同样的批次/运行记录编号开始。"""
    from app.seed import SEED_ROWS

    store._tables["batch_queue"] = [dict(row) for row in SEED_ROWS["batch_queue"]]
    store._tables["run_history"] = [dict(row) for row in SEED_ROWS["run_history"]]
    store._tables["error_recover"] = [dict(row) for row in SEED_ROWS["error_recover"]]


def _receipt(source_id: int, entry_id: int) -> dict:
    # 与真实回执一致：采集来源字段在前，被截断时仍可关联。
    return {
        "source_module": "batch_queue", "source_id": source_id,
        "module": "run_history", "entity": "运行记录", "entry_id": entry_id,
        "status": "已完成", "version": "", "action": "auto_create_from_batch",
        "timestamp": "2026-01-01T00:00:00", "abnormal": False,
    }


def _write_three_intact_and_half_fourth() -> str:
    """保留三条完好记录，让第四条只写半行。"""
    path = persist.data_path("runs", RUN_LOG)
    with open(path, "w", encoding="utf-8") as f:
        for i in range(1, 4):
            f.write(json.dumps(_receipt(i, i), ensure_ascii=False) + "\n")
        half = json.dumps(_receipt(4, 4), ensure_ascii=False)[:110]
        assert not half.endswith("}")
        f.write(half)
    return path


def _batch_runs(source_id: int | None = None) -> list[dict]:
    runs = persist.read_jsonl("runs", RUN_LOG)
    return [
        r for r in runs
        if r.get("source_module") == "batch_queue"
        and (source_id is None or str(r.get("source_id")) == str(source_id))
    ]


def test_corrupt_tail_does_not_break_history_read():
    """损坏尾行只产生待核记录，完整历史仍可持久读取。"""
    _reset_runs_dir()
    _reset_store()
    _write_three_intact_and_half_fourth()

    service = RunHistoryService()
    recent = service.list_recent_with_pending()

    assert len(persist.read_jsonl("runs", RUN_LOG)) == 3
    assert len(recent["items"]) >= 3
    pending = recent["pending_receipts"]
    assert any(p["partial"].get("source_id") == 4 for p in pending), pending

    snapshot = service.domain_snapshot()
    assert snapshot["file_records"] == 3
    assert snapshot["corrupt_lines"] == 1
    assert snapshot["pending_receipts"] == 1


def test_new_batch_confirmation_appends_cleanly_after_corrupt_tail():
    """读取时提交新的批次确认，回执接在完整记录之后而不是拼到断尾上。"""
    _reset_runs_dir()
    _reset_store()
    path = _write_three_intact_and_half_fourth()
    service = RunHistoryService()
    service.list_recent_with_pending()  # 读取时读到断尾

    batch_service = BatchQueueService()
    # 原批次（断尾回执属于它）先登记但还没补交，新批次另起一号。
    original, _ = batch_service.create_entry({
        "批次编号": "BATC-REG-OLD", "关联计划": "PLAN-OLD", "任务数量": 3,
    })
    assert original["id"] == 4
    new_batch, _ = batch_service.create_entry({
        "批次编号": "BATC-REG-001", "关联计划": "PLAN-REG", "任务数量": 6,
    })
    entry, message = batch_service.run_action(new_batch["id"], "确认完成")
    assert entry is not None, message

    # 断尾被隔离，主日志每条都是完整 JSON。
    with open(path, encoding="utf-8") as f:
        lines = [line for line in f.read().splitlines() if line.strip()]
    assert len(lines) == 4
    for line in lines:
        json.loads(line)
    assert os.path.exists(path + ".corrupt")

    # 新回执可读且能关联到自己的采集来源。
    new_receipts = _batch_runs(new_batch["id"])
    assert len(new_receipts) == 1
    assert new_receipts[0]["status"] == "已完成"

    # 旧断尾仍作为待核记录保留，来源可核对。
    pending = service.get_pending_receipts()
    assert [p["partial"].get("source_id") for p in pending] == [4]


def test_resubmit_original_confirmation_once_per_source():
    """补交原确认与再次确认都只结算一次。"""
    _reset_runs_dir()
    _reset_store()
    _write_three_intact_and_half_fourth()

    batch_service = BatchQueueService()
    original, _ = batch_service.create_entry({
        "批次编号": "BATC-REG-004", "关联计划": "PLAN-REG", "任务数量": 3,
    })
    assert original["id"] == 4, "测试假设原断尾对应第4个批次"

    service = RunHistoryService()
    first_result, first_message = service.resubmit_receipt("batch_queue", 4)
    assert first_result is not None and first_result["settled"] is True, first_message
    assert len(_batch_runs(4)) == 1
    assert service.get_pending_receipts() == []

    # 补交一次原确认后，再次补交 / 再次确认都不再追加。
    second_result, _ = service.resubmit_receipt("batch_queue", 4)
    assert second_result is not None and second_result["settled"] is False
    batch_service.run_action(4, "确认完成")
    assert len(_batch_runs(4)) == 1

    new_batch, _ = batch_service.create_entry({
        "批次编号": "BATC-REG-005", "关联计划": "PLAN-REG-2", "任务数量": 5,
    })
    batch_service.run_action(new_batch["id"], "确认完成")
    batch_service.run_action(new_batch["id"], "确认完成")
    resubmitted, _ = service.resubmit_receipt("batch_queue", new_batch["id"])
    assert resubmitted is not None and resubmitted["settled"] is False
    assert len(_batch_runs(new_batch["id"])) == 1

    # 新旧批次各自结算一次，完好条目一直可查。
    assert len(_batch_runs(4)) == 1
    assert len(_batch_runs(new_batch["id"])) == 1
    assert len(persist.read_jsonl("runs", RUN_LOG)) == 5


def test_fused_corrupt_line_keeps_sources_apart():
    """旧尾行与新回执拼在同一行时，不能当作成功记录读取。"""
    _reset_runs_dir()
    _reset_store()
    path = persist.data_path("runs", RUN_LOG)
    fused = (
        '{"module": "run_history", "entity": "运行记录", "entry_id": 4, "status": "运'
        + json.dumps(_receipt(7, 7), ensure_ascii=False)
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(json.dumps(_receipt(1, 1), ensure_ascii=False) + "\n")
        f.write(fused + "\n")

    records, issues = persist.read_jsonl_with_issues("runs", RUN_LOG)
    assert len(records) == 1 and records[0]["source_id"] == 1
    assert len(issues) == 1
    assert persist.has_settled_source("runs", RUN_LOG, "batch_queue", 1) is True
    assert persist.has_settled_source("runs", RUN_LOG, "batch_queue", 7) is False
    assert [p["partial"]["source_id"] for p in persist.pending_receipts("runs", RUN_LOG)] == [7]


if __name__ == "__main__":
    test_corrupt_tail_does_not_break_history_read()
    print("PASS: test_corrupt_tail_does_not_break_history_read")
    test_new_batch_confirmation_appends_cleanly_after_corrupt_tail()
    print("PASS: test_new_batch_confirmation_appends_cleanly_after_corrupt_tail")
    test_resubmit_original_confirmation_once_per_source()
    print("PASS: test_resubmit_original_confirmation_once_per_source")
    test_fused_corrupt_line_keeps_sources_apart()
    print("PASS: test_fused_corrupt_line_keeps_sources_apart")
    print("\nALL 4 TESTS PASSED")
