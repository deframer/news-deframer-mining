import json
import importlib.util
import logging
from pathlib import Path
import sys


_BASE_FIELDS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "asctime",
    "taskName",
}


class SilentLogger:
    """Simple logger drop-in that ignores all log messages."""

    def __init__(self, name: str = "news_deframer.silent") -> None:
        self.name = name

    def debug(self, *args, **kwargs) -> None:  # noqa: D401 - intentionally empty
        return None

    def info(self, *args, **kwargs) -> None:
        return None

    def warning(self, *args, **kwargs) -> None:
        return None

    def error(self, *args, **kwargs) -> None:
        return None

    def exception(self, *args, **kwargs) -> None:
        return None

    def log(self, *args, **kwargs) -> None:
        return None

    def getChild(self, name: str) -> "SilentLogger":  # noqa: N802 - mimic logging API
        return self

    def isEnabledFor(self, level: int) -> bool:  # noqa: N802
        return False


class ExtraFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        base = super().format(record)
        extra_keys = [k for k in record.__dict__ if k not in _BASE_FIELDS]
        if extra_keys:
            extra_payload = {key: record.__dict__[key] for key in extra_keys}
            cleaned_payload = {
                key: value for key, value in extra_payload.items() if value is not None
            }
            try:
                serialized = json.dumps(
                    cleaned_payload, default=str, ensure_ascii=False
                )
            except TypeError:
                serialized = str(cleaned_payload)
            base = f"{base} | extra={serialized}"
        return base


def _format_bytes(size_bytes: int | None) -> str:
    if size_bytes is None:
        return "unknown"

    size = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1000.0 or unit == "TB":
            return f"{size:.1f}{unit}"
        size /= 1000.0

    return "unknown"


def _get_rss_bytes() -> int | None:
    try:
        with Path("/proc/self/status").open("r", encoding="utf-8") as status_file:
            for line in status_file:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) * 1024
    except OSError:
        return None
    return None


def _get_package_size_bytes(package_name: str) -> int | None:
    spec = importlib.util.find_spec(package_name)
    locations = getattr(spec, "submodule_search_locations", None) if spec else None
    if not locations:
        return None

    package_dir = next(iter(locations), None)
    if package_dir is None:
        return None

    root = package_dir if isinstance(package_dir, str) else str(package_dir)
    total = 0
    for entry in Path(root).rglob("*"):
        if entry.is_file():
            try:
                total += entry.stat().st_size
            except FileNotFoundError:
                continue
    return total or None


def configure_logging(log_level: str = "INFO", stream=sys.stdout) -> None:
    """Configures the root logger and ensures extra data is visible."""
    level = getattr(logging, log_level.upper(), logging.INFO)

    handler = logging.StreamHandler(stream)
    handler.setFormatter(
        ExtraFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )

    logging.basicConfig(level=level, handlers=[handler], force=True)
