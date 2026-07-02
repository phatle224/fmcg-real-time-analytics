import asyncio
import json
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from schemas import POSTransaction, StreamStatusResponse
from simulator import generate_batch

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "pos.transactions")
BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "100"))
THROUGHPUT_TPS          = int(os.getenv("THROUGHPUT_TPS", "1000"))

# ── State ──────────────────────────────────────────────────────────────────────
_producer: KafkaProducer | None = None
_streaming: bool = False
_current_tps: int = 0
_total_sent: int = 0


def _get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=5,
            batch_size=32768,
            linger_ms=10,
            compression_type="gzip",
        )
    return _producer


def _serialize(tx: POSTransaction) -> dict:
    d = tx.model_dump()
    d["timestamp"] = tx.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return d


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm up producer on startup
    try:
        _get_producer()
    except NoBrokersAvailable:
        pass  # Will retry lazily
    yield
    if _producer:
        _producer.flush()
        _producer.close()


app = FastAPI(
    title="FMCG POS Event Generator",
    description="Simulates Vinamilk POS transactions — 1,000 tx/s",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/health", tags=["ops"])
def health():
    return {
        "status": "ok",
        "kafka_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
        "streaming": _streaming,
        "total_sent": _total_sent,
    }


# ── One-shot simulate ──────────────────────────────────────────────────────────
@app.post("/api/v1/simulate", tags=["events"])
def simulate(count: int = 100):
    """Send a one-time batch of N events to Kafka."""
    global _total_sent
    if count < 1 or count > 50000:
        raise HTTPException(status_code=400, detail="count must be 1–50000")

    producer = _get_producer()
    start    = time.perf_counter()
    batch    = generate_batch(count)

    for tx in batch:
        producer.send(KAFKA_TOPIC, value=_serialize(tx))
    producer.flush()

    elapsed      = time.perf_counter() - start
    _total_sent += count

    return {
        "sent": count,
        "elapsed_ms": round(elapsed * 1000, 2),
        "throughput_tps": round(count / elapsed),
        "total_sent": _total_sent,
    }


# ── Continuous stream ──────────────────────────────────────────────────────────
async def _stream_loop(tps: int):
    global _streaming, _total_sent
    producer    = _get_producer()
    batch_size  = min(tps, 500)
    interval    = batch_size / tps  # seconds between batches

    while _streaming:
        batch = generate_batch(batch_size)
        for tx in batch:
            producer.send(KAFKA_TOPIC, value=_serialize(tx))
        producer.flush()
        _total_sent += batch_size
        await asyncio.sleep(interval)


@app.post("/api/v1/stream/start", tags=["stream"])
async def start_stream(background_tasks: BackgroundTasks, tps: int = 1000):
    global _streaming, _current_tps
    if _streaming:
        return {"status": "already_running", "tps": _current_tps}
    if tps < 1 or tps > 5000:
        raise HTTPException(status_code=400, detail="tps must be 1–5000")
    _streaming   = True
    _current_tps = tps
    background_tasks.add_task(_stream_loop, tps)
    return {"status": "started", "tps": tps}


@app.post("/api/v1/stream/stop", tags=["stream"])
def stop_stream():
    global _streaming, _current_tps
    _streaming   = False
    _current_tps = 0
    return {"status": "stopped", "total_sent": _total_sent}


@app.get("/api/v1/stream/status", response_model=StreamStatusResponse, tags=["stream"])
def stream_status():
    return StreamStatusResponse(streaming=_streaming, tps=_current_tps)
