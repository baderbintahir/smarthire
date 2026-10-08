import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime

# One value per async task (= per request), unlike a global shared by all concurrent requests.
# Middleware sets it at the start of a request; every log line then picks it up automatically.
request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)


class JsonFormatter(logging.Formatter):
    """Renders each log record as one JSON object per line."""

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": request_id_var.get(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logging(service_name: str, level: str = "INFO") -> None:
    """Call once at service startup. Replaces existing handlers with one JSON handler on stdout
    (containers log to stdout; Docker collects it)."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service_name))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level.upper())
