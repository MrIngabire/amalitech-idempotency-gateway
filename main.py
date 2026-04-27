import asyncio
import hashlib
import json
import time
from typing import Dict, Any

from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel

app = FastAPI(title="Idempotency Gateway API")

# --- Developer's Choice: Key Expiration (TTL) ---
# We store the timestamp of when the key was created.
# This prevents our in-memory store from growing infinitely.
KEY_TTL_SECONDS = 3600 # 1 hour expiration

# In-memory database
# Structure: { "key": {"status": str, "payload_hash": str, "response": dict, "created_at": float} }
db: Dict[str, Any] = {}

class PaymentPayload(BaseModel):
    amount: float
    currency: str

def get_payload_hash(payload: PaymentPayload) -> str:
    """Generate a stable hash of the payload to detect modifications."""
    payload_str = json.dumps(payload.dict(), sort_keys=True)
    return hashlib.sha256(payload_str.encode()).hexdigest()

def clean_expired_keys():
    """Removes keys older than KEY_TTL_SECONDS."""
    current_time = time.time()
    expired_keys = [k for k, v in db.items() if current_time - v["created_at"] > KEY_TTL_SECONDS]
    for k in expired_keys:
        del db[k]

@app.post("/process-payment")
async def process_payment(
    payload: PaymentPayload,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key")
):
    # Housekeeping: remove old keys before processing
    clean_expired_keys()

    current_payload_hash = get_payload_hash(payload)

    # 1. Check if the Idempotency Key already exists
    if idempotency_key in db:
        record = db[idempotency_key]

        # User Story 3: Fraud/Error Check (Same key, different payload)
        if record["payload_hash"] != current_payload_hash:
            raise HTTPException(
                status_code=409,
                detail="Idempotency key already used for a different request body."
            )

        # Bonus Story: In-Flight Check
        # If the original request is still processing, wait for it to finish.
        while record["status"] == "processing":
            await asyncio.sleep(0.1)  # Poll state every 100ms

        # User Story 2: Duplicate Attempt (Return Cached Response)
        response.headers["X-Cache-Hit"] = "true"
        response.status_code = 200 # Returning 200 for a cached success
        return record["response"]

    # 2. First Request (Happy Path)
    # Lock the state immediately to 'processing'
    db[idempotency_key] = {
        "status": "processing",
        "payload_hash": current_payload_hash,
        "response": None,
        "created_at": time.time()
    }

    # Simulate external payment processing delay
    await asyncio.sleep(2.0)

    # Prepare the success response
    success_response = {"status": f"Charged {payload.amount} {payload.currency}"}

    # Save final state
    db[idempotency_key]["status"] = "completed"
    db[idempotency_key]["response"] = success_response

    response.status_code = 201
    return success_response

# Basic health check to pass the "Run Check" easily
@app.get("/")
def read_root():
    return {"message": "Idempotency Gateway is running. POST to /process-payment"}