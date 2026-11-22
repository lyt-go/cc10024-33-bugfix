"""共享文件持久化：跨模块数据产物读写。
data/runs/     → 运行历史JSONL
data/snapshots/ → 页面快照JSON
data/reports/   → 质量报告JSON
data/exports/   → 导出文件
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import threading
import time
from typing import Any

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_ROOT = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "data"))

# 同一本 JSONL 日志可能被多个请求同时追加（批次确认、恢复回执等）。
# 追加和损坏尾行修复必须是一个整体，否则两条回执仍可能交错写进同一行。
_WRITE_LOCK = threading.RLock()

# 截断尾行里能识别出的字段名，用来把断尾回执和采集来源关联起来。
_PARTIAL_KEYS = ("source_module", "source_id", "entry_id", "status", "action")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def data_path(sub: str, filename: str) -> str:
    p = os.path.join(_DATA_ROOT, sub, filename)
    _ensure_dir(os.path.dirname(p))
    return p


def write_json(sub: str, filename: str, data: dict[str, Any]) -> str:
    path = data_path(sub, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    return path


def read_json(sub: str, filename: str) -> dict[str, Any] | None:
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def partial_fields(raw: str) -> dict[str, Any]:
    """从无法解析的断尾文本里尽量还原采集来源字段。"""
    fields: dict[str, Any] = {}
    for key in _PARTIAL_KEYS:
        match = re.search(rf'"{re.escape(key)}"\s*:\s*(?:"([^"]*)"|(-?\d+))', raw)
        if not match:
            continue
        fields[key] = match.group(1) if match.group(1) is not None else int(match.group(2))
    return fields


def _source_keys(raw: str) -> list[tuple[str, str]]:
    """从断尾文本里还原所有采集来源。

    未修复的旧日志可能把旧尾行和新回执拼在同一行里，所以要逐段识别，
    而不是只取第一个 source 字段。
    """
    keys: list[tuple[str, str]] = []
    pattern = re.compile(
        r'"source_module"\s*:\s*"([^"]*)"[^{}]*?"source_id"\s*:\s*(-?\d+)'
    )
    for match in pattern.finditer(raw):
        keys.append((match.group(1), match.group(2)))
    if not keys:
        fields = partial_fields(raw)
        if "source_module" in fields and "source_id" in fields:
            keys.append((str(fields["source_module"]), str(fields["source_id"])))
    return keys


def _quarantine_tail(path: str, tail: bytes) -> str:
    """把损坏的断尾另存到旁路文件，主日志只保留完整记录。"""
    quarantine_path = path + ".corrupt"
    # 隔离文件也按行保存，避免与上一次断尾粘成同一条记录。
    with open(quarantine_path, "ab") as f:
        if not tail.endswith(b"\n"):
            f.write(tail + b"\n")
        else:
            f.write(tail)
    return quarantine_path


def repair_jsonl_tail(sub: str, filename: str) -> str | None:
    """修复日志末尾没有换行收尾的半条记录。

    返回被隔离的断尾文本；如果文件本身完整则返回 None。
    完整但缺少换行的最后一条记录不会被隔离，只会补上换行。
    只回看文件尾部，不整段读入，长日志追加时也不放大内存。
    """
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return None
    with _WRITE_LOCK:
        with open(path, "rb+") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size == 0:
                return None
            f.seek(size - 1)
            if f.read(1) == b"\n":
                return None
            # 从尾部往前找最后一条完整记录的换行位置。
            chunk_size = 8192
            position = size
            last_newline = -1
            while position > 0 and last_newline < 0:
                start = max(0, position - chunk_size)
                f.seek(start)
                chunk = f.read(position - start)
                offset = chunk.rfind(b"\n")
                if offset >= 0:
                    last_newline = start + offset
                position = start
            tail_start = last_newline + 1
            f.seek(tail_start)
            tail = f.read(size - tail_start)
            if not tail.strip():
                # 只有空白，补一个换行即可。
                f.seek(0, os.SEEK_END)
                f.write(b"\n")
                return None
            try:
                json.loads(tail.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                _quarantine_tail(path, tail)
                f.truncate(tail_start)
                return tail.decode("utf-8", errors="replace")
            # 末行其实是完整 JSON，只是缺少换行符。
            f.seek(0, os.SEEK_END)
            f.write(b"\n")
            return None


_HELD_FILE_LOCKS = threading.local()


@contextlib.contextmanager
def _file_lock(path: str):
    """跨进程互斥：同一本日志的修复与追加必须串行。

    同一线程里可重入——结算逻辑先拿到锁，再调用 append_jsonl 时不会自锁。
    """
    held = getattr(_HELD_FILE_LOCKS, "paths", None)
    if held is None:
        held = set()
        _HELD_FILE_LOCKS.paths = held
    if path in held:
        yield
        return
    lock_path = path + ".lock"
    _ensure_dir(os.path.dirname(lock_path))
    with open(lock_path, "a+b") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        held.add(path)
        try:
            yield
        finally:
            held.discard(path)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def append_jsonl(sub: str, filename: str, record: dict[str, Any]) -> str:
    """追加一条 JSONL 记录。

    如果日志末尾有被截断的半条记录，先把它隔离到 `<filename>.corrupt`，
    再从完整记录之后另起一行写入。这样损坏内容不会和新回执拼成一条
    “成功记录”，已有完整记录也仍然可以持续追加和读取。
    """
    path = data_path(sub, filename)
    payload = (json.dumps(record, ensure_ascii=False, default=str) + "\n").encode("utf-8")
    with _WRITE_LOCK, _file_lock(path):
        repair_jsonl_tail(sub, filename)
        with open(path, "ab") as f:
            f.write(payload)
    return path


def receipt_lock() -> threading.RLock:
    """跨模块回执结算共用的进程内可重入锁，保证“判重 + 追加”是一个原子步骤。"""
    return _WRITE_LOCK


def journal_lock(sub: str, filename: str):
    """回执结算的跨进程文件锁（与进程内锁配合使用）。"""
    return _file_lock(data_path(sub, filename))


def read_jsonl_with_issues(
    sub: str, filename: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """容错读取 JSONL，返回（完整记录, 损坏行信息）。

    单行损坏不会中断整段历史的读取。损坏行会带上从断尾里能还原出的
    来源字段，供运行历史作为待核记录排查。
    """
    path = data_path(sub, filename)
    records: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    if not os.path.exists(path):
        return records, issues
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line_number, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except (json.JSONDecodeError, ValueError) as exc:
                issues.append({
                    "line": line_number,
                    "raw": line[:500],
                    "error": str(exc),
                    "partial": partial_fields(line),
                })
                continue
            if not isinstance(value, dict):
                issues.append({
                    "line": line_number,
                    "raw": line[:500],
                    "error": "记录不是JSON对象",
                    "partial": {},
                })
                continue
            records.append(value)
    return records, issues


def read_jsonl(sub: str, filename: str) -> list[dict[str, Any]]:
    """读取 JSONL 中的完整记录；损坏行被跳过而不是让整段读取失败。"""
    records, _ = read_jsonl_with_issues(sub, filename)
    return records


def _source_key(record: dict[str, Any]) -> tuple[str, str] | None:
    source_module = record.get("source_module")
    source_id = record.get("source_id")
    if source_module is None or source_id is None:
        return None
    return str(source_module), str(source_id)


def settled_sources(sub: str, filename: str) -> set[tuple[str, str]]:
    """所有已经完整落盘的回执所对应的采集来源。"""
    return {
        key for record in read_jsonl(sub, filename)
        if (key := _source_key(record)) is not None
    }


def has_settled_source(
    sub: str, filename: str, source_module: str, source_id: Any
) -> bool:
    """判断某个采集来源是否已经有一份完整的结算回执。"""
    return (str(source_module), str(source_id)) in settled_sources(sub, filename)


def corrupt_records(sub: str, filename: str) -> list[dict[str, Any]]:
    """读取断尾回执（含尚未修复的日志尾行），供核对与补交时按采集来源关联。

    `.corrupt` 可能积累多次截断的尾部，每条断尾单独一行保存。拼接在同一行
    里的新旧回执会拆成多条，各自带上能识别出的采集来源。
    """
    raws: list[str] = []
    path = data_path(sub, filename) + ".corrupt"
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            raws.extend(raw.strip() for raw in f if raw.strip())
    _, issues = read_jsonl_with_issues(sub, filename)
    raws.extend(issue["raw"] for issue in issues if issue["partial"] or issue["raw"])

    records: list[dict[str, Any]] = []
    for raw in raws:
        keys = _source_keys(raw)
        if not keys:
            records.append({"raw": raw, "partial": partial_fields(raw)})
            continue
        for source_module, source_id in keys:
            records.append({
                "raw": raw,
                "partial": {"source_module": source_module, "source_id": int(source_id)}
                if source_id.lstrip("-").isdigit() else
                {"source_module": source_module, "source_id": source_id},
            })
    return records


def pending_receipts(sub: str, filename: str) -> list[dict[str, Any]]:
    """断尾回执中尚未被完整回执补交的采集来源（按来源去重）。"""
    settled = settled_sources(sub, filename)
    pending: list[dict[str, Any]] = []
    seen: set[tuple[str, str] | str] = set()
    for item in corrupt_records(sub, filename):
        key = _source_key(item["partial"])
        identity: tuple[str, str] | str = key if key is not None else item["raw"]
        if identity in seen or (key is not None and key in settled):
            continue
        seen.add(identity)
        pending.append(item)
    return pending

def list_files(sub: str, suffix: str = "") -> list[str]:
    path = os.path.join(_DATA_ROOT, sub)
    if not os.path.isdir(path):
        return []
    names = sorted(os.listdir(path))
    if suffix:
        names = [n for n in names if n.endswith(suffix)]
    return names


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")
