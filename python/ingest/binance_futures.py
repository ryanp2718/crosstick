"""Binance USDⓈ-M perpetual-futures ingester — slice 1: liquidations + mark/funding.

Feed: wss://fstream.binance.com/market/stream (combined streams, no auth).
Design + verified wire contract: docs/DESIGN_perp_capture.md.

Slice 1 is stateless on purpose: no order book, no REST snapshot, no sequence
validation — both streams are venue-computed snapshots, so a reconnect (Binance
force-closes at 24h) costs only the gap window. bootstrap() therefore flips
straight to LIVE.

Wire notes:
  - Routed paths are now required; all streams are known at construction, so we
    connect to /market/stream?streams=... and never send live SUBSCRIBE
    messages (build_subscribe_messages returns []).
  - Combined-stream frames are enveloped {"stream": ..., "data": {...}}.
  - forceOrder is SAMPLED: largest liquidation per symbol per 1000ms only.
  - markPrice@1s ticks every second per symbol, which makes the staleness
    watchdog meaningful even when no liquidations occur.
"""
from __future__ import annotations

import logging

import msgspec

from common.kafka_io import liquidation_topic, markprice_topic
from common.models import Liquidation, MarkPrice, Side
from ingest.base_ingester import BaseIngester, ParsedEvent, SymbolContext, SymbolState

log = logging.getLogger(__name__)

DEFAULT_WS_BASE = "wss://fstream.binance.com/market/stream"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
DEFAULT_STALE_TIMEOUT = 10.0  # markPrice@1s guarantees ~1 frame/s/symbol when healthy


def stream_names(symbols: list[str]) -> list[str]:
    return [name for sym in symbols for name in
            (f"{sym.lower()}@forceOrder", f"{sym.lower()}@markPrice@1s")]


# ─── wire structs (msgspec) ──────────────────────────────────────────────────


class _Envelope(msgspec.Struct):
    """Combined-stream frame; `data` stays raw until discriminated by `e`."""
    stream: str = ""
    data: msgspec.Raw = msgspec.Raw(b"")


class _Disc(msgspec.Struct):
    e: str = ""


class _ForceOrder(msgspec.Struct):
    """Inner `o` object of a forceOrder event."""
    s: str
    S: str
    q: str
    p: str
    ap: str
    X: str
    z: str
    T: int = 0


class _ForceOrderEvent(msgspec.Struct):
    o: _ForceOrder
    E: int = 0


class _MarkPriceEvent(msgspec.Struct):
    s: str
    p: str
    i: str
    r: str
    T: int = 0
    P: str = ""
    E: int = 0


_ENV_DEC = msgspec.json.Decoder(_Envelope)
_DISC_DEC = msgspec.json.Decoder(_Disc)
_FORCE_DEC = msgspec.json.Decoder(_ForceOrderEvent)
_MARK_DEC = msgspec.json.Decoder(_MarkPriceEvent)


# ─── ingester ────────────────────────────────────────────────────────────────


class BinanceFuturesIngester(BaseIngester):
    def __init__(
        self,
        *,
        producer,
        symbols: list[str] | None = None,
        ws_base: str = DEFAULT_WS_BASE,
        stale_timeout: float | None = DEFAULT_STALE_TIMEOUT,
        **kw: object,
    ) -> None:
        symbols = symbols if symbols is not None else list(DEFAULT_SYMBOLS)
        super().__init__(
            exchange="binance-futures",
            symbols=symbols,
            ws_url=f"{ws_base}?streams={'/'.join(stream_names(symbols))}",
            producer=producer,
            stale_timeout=stale_timeout,
            **kw,
        )

    # ─── hooks ───────────────────────────────────────────────────────────────

    async def bootstrap(self, symbol: str) -> None:
        # Stateless streams: nothing to fetch, no book to warm.
        self.contexts[symbol].set_state(SymbolState.LIVE, reason="stateless stream")

    def build_subscribe_messages(self) -> list[str]:
        return []  # streams are in the connection URL

    def parse_message(self, raw: bytes, local_recv_ts_ns: int) -> list[ParsedEvent]:
        env = _ENV_DEC.decode(raw)
        if not env.stream:
            return []  # not a combined-stream frame (control/unknown)
        data = bytes(env.data)
        kind = _DISC_DEC.decode(data).e
        if kind == "forceOrder":
            ev = _FORCE_DEC.decode(data)
            return [ParsedEvent(
                symbol=ev.o.s, kind="liquidation", payload=ev.o,
                raw_bytes=len(raw), exchange_ts_ns=ev.o.T * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        if kind == "markPriceUpdate":
            m = _MARK_DEC.decode(data)
            return [ParsedEvent(
                symbol=m.s, kind="mark_price", payload=m,
                raw_bytes=len(raw), exchange_ts_ns=m.E * 1_000_000,
                local_recv_ts_ns=local_recv_ts_ns,
            )]
        return []

    async def process_event(self, ctx: SymbolContext, event: ParsedEvent) -> None:
        if event.kind == "liquidation":
            o = event.payload
            await self._emit(
                liquidation_topic(self.exchange, ctx.symbol),
                Liquidation(
                    exchange=self.exchange, symbol=ctx.symbol,
                    side=Side.ASK if o.S == "SELL" else Side.BID,
                    price=o.p, avg_price=o.ap, orig_size=o.q, filled_size=o.z,
                    status=o.X,
                    exchange_ts_ns=event.exchange_ts_ns,
                    local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
        elif event.kind == "mark_price":
            m = event.payload
            await self._emit(
                markprice_topic(self.exchange, ctx.symbol),
                MarkPrice(
                    exchange=self.exchange, symbol=ctx.symbol,
                    mark_price=m.p, index_price=m.i, est_settle_price=m.P,
                    funding_rate=m.r, next_funding_ts_ns=m.T * 1_000_000,
                    exchange_ts_ns=event.exchange_ts_ns,
                    local_ts_ns=event.local_recv_ts_ns,
                ),
                ctx.symbol, event,
            )
