import base64
import math
import threading
import time
import uuid
from collections import defaultdict, deque

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

TOTAL = 53          # T: fixed catalog of order ids 1..T
RATE_LIMIT = 17     # R requests per window
WINDOW = 10.0       # seconds
MAX_LIMIT = 100


# ---------------------------------------------------------------- helpers
def encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(f"o:{offset}".encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> int:
    raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    if not raw.startswith("o:"):
        raise ValueError("bad cursor")
    offset = int(raw[2:])
    if offset < 0:
        raise ValueError("bad cursor")
    return offset


def get_page(limit: int, cursor):
    offset = decode_cursor(cursor) if cursor else 0
    ids = range(offset + 1, min(offset + limit, TOTAL) + 1)
    items = [{"id": i, "status": "created", "item": f"item-{i}"} for i in ids]
    next_offset = offset + len(items)
    next_cursor = encode_cursor(next_offset) if next_offset < TOTAL else None
    return {"items": items, "next_cursor": next_cursor}


class RateLimiter:
    """Sliding-window log, one bucket per client id."""

    def __init__(self, limit: int, window: float):
        self.limit, self.window = limit, window
        self.hits = defaultdict(deque)
        self.lock = threading.Lock()

    def check(self, client_id: str, now=None):
        """Returns (allowed, retry_after_seconds)."""
        now = time.monotonic() if now is None else now
        with self.lock:
            q = self.hits[client_id]
            while q and now - q[0] >= self.window:
                q.popleft()
            if len(q) >= self.limit:
                return False, max(1, math.ceil(q[0] + self.window - now))
            q.append(now)
            return True, 0


limiter = RateLimiter(RATE_LIMIT, WINDOW)

# idempotency store: key -> (status, body); orders created by POST live here,
# separate from the fixed 1..T catalog served by GET /orders
IDEMPOTENT = {}
ID_LOCK = threading.Lock()

# ---------------------------------------------------------------- app
app = FastAPI()


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    client_id = request.headers.get("x-client-id")
    if client_id and request.method != "OPTIONS" and request.url.path.startswith("/orders"):
        allowed, retry_after = limiter.check(client_id)
        if not allowed:
            return JSONResponse(
                {"error": "rate limit exceeded", "retry_after": retry_after},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )
    return await call_next(request)


# added last, so it is outermost and CORS headers are on every response (incl. 429)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After", "Idempotent-Replayed"],
)


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/orders")
async def create_order(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    key = request.headers.get("idempotency-key")

    with ID_LOCK:
        if key and key in IDEMPOTENT:
            status, body = IDEMPOTENT[key]
            return JSONResponse(body, status_code=status, headers={"Idempotent-Replayed": "true"})
        order = {
            "id": f"ord_{uuid.uuid4().hex[:12]}",
            "status": "created",
            "created_at": time.time(),
            "data": payload if isinstance(payload, dict) else {},
        }
        if key:
            IDEMPOTENT[key] = (201, order)
    return JSONResponse(order, status_code=201)


@app.get("/orders")
def list_orders(limit: int = 10, cursor: str = None):
    limit = max(1, min(limit, MAX_LIMIT))
    try:
        return get_page(limit, cursor)
    except Exception:
        return JSONResponse({"error": "invalid cursor"}, status_code=400)
