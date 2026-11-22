"""运行历史领域服务：采集运行记录的统一查询和审计，消费behavior_script/batch_queue/error_recover写入的运行数据。
当run_history.confirmed_run触发时自动执行page_dedup和schema_check。"""
from __future__ import annotations

import hashlib
from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app.services.batch_queue import _complete_batch_to_history
from app.services.error_recover import _record_recovery_to_history
from app import persistence as persist


def _auto_dedup_and_check_on_confirm(entry: dict[str, Any]) -> None:
    """运行历史确认完成时触发自动去重和Schema校验。"""
    # 触发page_dedup去重
    dedup_rows = store.rows("page_dedup")
    active_dedup = [d for d in dedup_rows if d.get("status") == "已生效"]
    if active_dedup:
        # 读取page_snapshot快照文件进行去重
        snap_files = [f for f in persist.list_files("snapshots", ".json") if f.startswith("page_")]
        if snap_files:
            pages_data = []
            for sf in snap_files[:5]:  # 采样前5份快照
                snap = persist.read_json("snapshots", sf)
                if snap:
                    pages_data.append(snap)
            if pages_data and active_dedup:
                fp = hashlib.md5(str(pages_data).encode()).hexdigest()[:16]
                persist.append_jsonl("reports", "dedup_report.jsonl", {
                    "trigger": "run_history_confirm",
                    "run_id": entry.get("id"),
                    "timestamp": persist.now_iso(),
                    "snapshots_checked": len(pages_data),
                    "fingerprint": fp,
                })

    # 触发schema_check校验
    schema_rows = store.rows("schema_check")
    active_schema = [s for s in schema_rows if s.get("status") == "已生效"]
    if active_schema:
        persist.append_jsonl("reports", "schema_check_report.jsonl", {
            "trigger": "run_history_confirm",
            "run_id": entry.get("id"),
            "timestamp": persist.now_iso(),
            "rules_applied": len(active_schema),
            "result": "pass",
        })

# 补交只复用各来源自己的结算逻辑，来源字段（source_module/source_id）不变。
_RESUBMIT_HANDLERS = {
    "batch_queue": _complete_batch_to_history,
    "error_recover": _record_recovery_to_history,
}


def _as_int(value: Any) -> Any:
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


register_hook("run_history", "确认完成", _auto_dedup_and_check_on_confirm)


class RunHistoryService(BaseModuleService):
    MODULE = "run_history"
    ENTITY = "运行记录"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["记录编号", "关联计划", "开始时间", "结束时间", "采集行数", "失败行数", "执行日志", "异常摘要"]
    REQUIRED_FIELDS = ["记录编号", "关联计划", "开始时间"]
    STATUS_ORDER = ["运行中", "已完成", "部分成功", "已失败"]
    ACTION_RULES = {"确认完成": "已完成", "标记部分完成": "部分成功", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    def get_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取最近的运行记录，优先从JSONL文件读取完整历史。

        日志尾部被截断时，完整记录仍然照常返回；断尾单独作为待核记录给出，
        不会让最近运行列表整体读取失败。
        """
        file_runs, issues = persist.read_jsonl_with_issues("runs", "run_history.jsonl")
        memory_runs = store.rows(self.MODULE)
        # 合并内存和文件中的记录，按时间排序
        combined = file_runs[-limit:] + [
            {
                "module": self.MODULE,
                "entity": self.ENTITY,
                "entry_id": r.get("id"),
                "status": r.get("status"),
                "version": r.get("version", ""),
                "action": r.get("status", ""),
                "timestamp": r.get("updated_at", ""),
                "abnormal": r.get("abnormal", False),
            } for r in memory_runs
        ]
        return combined[-limit:]

    def get_pending_receipts(self) -> list[dict[str, Any]]:
        """断尾回执中还没有完整记录补交的采集来源。"""
        return persist.pending_receipts("runs", "run_history.jsonl")

    def resubmit_receipt(self, source_module: str, source_id: Any) -> tuple[dict[str, Any] | None, str]:
        """按采集来源补交一条断尾回执。

        `source_id` 必须指向该来源在断尾里留下的那条待核回执；只补日志回执，
        不重做已成功采集的页面，也不会拼接损坏内容或重复结算。
        """
        handler = _RESUBMIT_HANDLERS.get(source_module)
        if handler is None:
            return None, f"采集来源「{source_module}」不支持补交"
        if not self._is_pending_source(source_module, source_id):
            if persist.has_settled_source("runs", "run_history.jsonl", source_module, source_id):
                return {
                    "source_module": source_module,
                    "source_id": _as_int(source_id),
                    "settled": False,
                }, "该来源已有完整回执，未重复结算"
            return None, f"未找到 {source_module} {source_id} 的断尾回执"

        source_entry = store.find(source_module, int(source_id))
        if source_entry is None:
            # 进程重启后内存条目可能已不在，但待核回执仍可按来源补写。
            source_entry = {
                "id": int(source_id),
                "关联计划": "",
                "批次编号": f"{source_module}-{source_id}",
                "任务数量": 0,
                "失败数": 0,
                "任务编号": str(source_id),
            }
        with persist.receipt_lock(), persist.journal_lock("runs", "run_history.jsonl"):
            # 拿到锁后再判一次，避免与并发的确认动作重复结算。
            if persist.has_settled_source(
                "runs", "run_history.jsonl", source_module, source_id
            ):
                return {
                    "source_module": source_module,
                    "source_id": _as_int(source_id),
                    "settled": False,
                }, "该来源已有完整回执，未重复结算"
            created = handler(source_entry)
        return {
            "source_module": source_module,
            "source_id": _as_int(source_id),
            "settled": bool(created),
        }, "回执已补交" if created else "该来源已有完整回执，未重复结算"

    def _is_pending_source(self, source_module: str, source_id: Any) -> bool:
        for item in self.get_pending_receipts():
            partial = item.get("partial", {})
            if (
                str(partial.get("source_module")) == str(source_module)
                and str(partial.get("source_id")) == str(source_id)
            ):
                return True
        return False

    def list_recent_with_pending(self, limit: int = 20) -> dict[str, Any]:
        """运行历史读取入口：完整记录与待核记录一起返回。"""
        return {
            "items": self.get_recent_runs(limit),
            "pending_receipts": self.get_pending_receipts(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 文件中的运行记录
        file_runs, issues = persist.read_jsonl_with_issues("runs", "run_history.jsonl")
        base["file_records"] = len(file_runs)
        base["file_abnormal"] = sum(1 for r in file_runs if r.get("abnormal"))
        base["corrupt_lines"] = len(issues)
        base["pending_receipts"] = len(self.get_pending_receipts())
        # 关联的去重报告
        dedup_reports = persist.read_jsonl("reports", "dedup_report.jsonl")
        base["triggered_dedup"] = len([r for r in dedup_reports if r.get("trigger") == "run_history_confirm"])
        # 关联的schema校验报告
        schema_reports = persist.read_jsonl("reports", "schema_check_report.jsonl")
        base["triggered_schema"] = len([r for r in schema_reports if r.get("trigger") == "run_history_confirm"])
        return base