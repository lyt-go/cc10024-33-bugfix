"""批次队列领域服务：采集任务批次管理，消费script_schedule调度命令，写入run_history运行记录。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _complete_batch_to_history(entry: dict[str, Any]) -> bool:
    """批次确认完成时写入run_history，同一采集来源只结算一次。

    返回 False 表示该批次此前已经有一条完整回执（例如补交过），本次不再
    重复追加运行记录；批次状态本身仍由动作层照常流转。
    """
    if persist.has_settled_source(
        "runs", "run_history.jsonl", "batch_queue", entry.get("id")
    ):
        return False
    with persist.receipt_lock(), persist.journal_lock("runs", "run_history.jsonl"):
        if persist.has_settled_source(
            "runs", "run_history.jsonl", "batch_queue", entry.get("id")
        ):
            return False
        run_rows = store.rows("run_history")
        # 确认动作本身可能已经成功、只是回执日志被截断：此时沿用内存里
        # 那条运行记录，补交只补日志回执，不重复造一条运行历史。
        existing = next(
            (r for r in run_rows
             if r.get("source_module") == "batch_queue"
             and str(r.get("source_id")) == str(entry.get("id"))),
            None,
        )
        if existing is None:
            # 旧版本写入的运行记录没有来源字段，按执行日志里的批次编号认领。
            marker = f"批次{entry.get('批次编号')}执行完成"
            existing = next(
                (r for r in run_rows
                 if not r.get("source_module") and r.get("执行日志") == marker),
                None,
            )
            if existing is not None:
                existing["source_module"] = "batch_queue"
                existing["source_id"] = entry.get("id")
        new_id = existing.get("id") if existing else (
            max((int(r.get("id", 0)) for r in run_rows), default=0) + 1
        )
        if existing is None:
            record = {
                "id": new_id,
                "记录编号": f"RUN-BATCH-{new_id:04d}",
                "关联计划": entry.get("关联计划", ""),
                "开始时间": persist.now_iso(),
                "结束时间": persist.now_iso(),
                "采集行数": str(entry.get("任务数量", 0)),
                "失败行数": str(entry.get("失败数", 0)),
                "执行日志": f"批次{entry.get('批次编号')}执行完成",
                "异常摘要": "",
                "status": "已完成",
                "pending": False,
                "abnormal": False,
                "version": "",
                "updated_at": persist.now_iso(),
                "source_module": "batch_queue",
                "source_id": entry.get("id"),
            }
            run_rows.append(record)
        persist.append_jsonl("runs", "run_history.jsonl", {
            # 来源字段放在最前：回执即使被截断，也能据此关联到原批次。
            "source_module": "batch_queue", "source_id": entry.get("id"),
            "module": "run_history", "entity": "运行记录", "entry_id": new_id,
            "status": "已完成", "version": "", "action": "auto_create_from_batch",
            "timestamp": persist.now_iso(), "abnormal": False,
        })
    return True


register_hook("batch_queue", "确认完成", _complete_batch_to_history)


class BatchQueueService(BaseModuleService):
    MODULE = "batch_queue"
    ENTITY = "采集批次"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["批次编号", "关联计划", "任务数量", "完成数", "失败数", "批次状态", "截止时间", "优先级"]
    REQUIRED_FIELDS = ["批次编号", "关联计划", "任务数量"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已超时"]
    ACTION_RULES = {"发起执行": "执行中", "确认完成": "已完成", "标记超时": "已超时"}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        batches = store.rows(self.MODULE)
        total_tasks = sum(int(r.get("任务数量", 0)) for r in batches)
        base["total_tasks"] = total_tasks
        # 关联的run_history
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        batch_runs = [r for r in runs if "batch" in str(r.get("action", ""))]
        base["batch_runs"] = len(batch_runs)
        # 关联的error_recover
        errors = store.rows("error_recover")
        batch_plans = {r.get("关联计划") for r in batches}
        base["recovery_linked"] = sum(1 for e in errors if e.get("关联计划") in batch_plans)
        return base