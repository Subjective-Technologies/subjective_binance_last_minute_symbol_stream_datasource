import sys
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from subjective_abstract_data_source_package import SubjectiveDataSource

from trading_contracts.market import utc_now
from trading_contracts.plugin_support import icon_for, symbols_from, ticker_stream


def _epoch(ts):
    if isinstance(ts, (int, float)):
        return float(ts) / 1000 if ts > 10_000_000_000 else float(ts)
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()


class SubjectiveLastMinuteSymbolStreamDataSource(SubjectiveDataSource):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.symbols = symbols_from(self._connection.get("symbols"))
        self.window_sec = max(1, int(self._connection.get("window_sec", 60)))
        self._events = defaultdict(deque)

    @classmethod
    def connection_schema(cls):
        return {
            "symbols": {"type": "textarea", "label": "Symbols", "required": True},
            "window_sec": {"type": "int", "label": "Window Seconds", "default": 60, "min": 1},
        }

    @classmethod
    def request_schema(cls):
        return {"events": {"type": "array", "label": "Market Events"}, "now": {"type": "text", "label": "Clock Override"}}

    @classmethod
    def output_schema(cls):
        return {
            "event": {"type": "object", "label": "Market Event"},
            "events": {"type": "array", "label": "Window Events"},
            "window_sec": {"type": "int", "label": "Window Seconds"},
            "error": {"type": "text", "label": "Error"},
        }

    @classmethod
    def icon(cls):
        return icon_for(__file__)

    def supports_streaming(self):
        return True

    def stream(self, request):
        request = request or {}
        now_value = request.get("now")
        for event in ticker_stream(request, {**self._connection, "symbols": self.symbols}, "multiple"):
            if event.get("event") is None and event.get("error"):
                yield {"event": None, "events": [], "window_sec": self.window_sec, "error": event["error"]}
                continue
            now_epoch = _epoch(now_value or event["ts"] or utc_now())
            values = self._events[event["symbol"]]
            values.append(event)
            cutoff = now_epoch - self.window_sec
            while values and _epoch(values[0]["ts"]) < cutoff:
                values.popleft()
            yield {"event": event, "events": list(values), "window_sec": self.window_sec, "error": ""}

    def run(self, request):
        result = {"event": None, "events": [], "window_sec": self.window_sec, "error": ""}
        for result in self.stream(request or {}):
            pass
        return result
