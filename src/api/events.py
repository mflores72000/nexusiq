import uuid
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import get_db

router = APIRouter(prefix="/events", tags=["Events"])


@router.get("/")
def list_events(
    domain: Optional[str] = Query(None),
    entity_id: Optional[str] = Query(None),
    entity_type: Optional[str] = Query(None),
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    filters = ["1=1"]
    params: dict = {"limit": limit, "offset": offset}

    if domain:
        filters.append("domain = :domain")
        params["domain"] = domain
    if entity_id:
        filters.append("entity_id = :entity_id")
        params["entity_id"] = entity_id
    if entity_type:
        filters.append("entity_type = :entity_type")
        params["entity_type"] = entity_type
    if from_ts:
        filters.append("timestamp >= :from_ts")
        params["from_ts"] = from_ts
    if to_ts:
        filters.append("timestamp <= :to_ts")
        params["to_ts"] = to_ts

    where = " AND ".join(filters)
    rows = db.execute(
        text(f"SELECT event_id, domain, case_id, entity_type, entity_id, activity, timestamp, ingested_at, data_quality FROM events WHERE {where} ORDER BY timestamp DESC LIMIT :limit OFFSET :offset"),
        params,
    ).fetchall()

    total = db.execute(
        text(f"SELECT COUNT(*) FROM events WHERE {where}"),
        {k: v for k, v in params.items() if k not in ("limit", "offset")},
    ).scalar()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "items": [
            {
                "event_id": str(r.event_id),
                "domain": r.domain,
                "case_id": r.case_id,
                "entity_type": r.entity_type,
                "entity_id": r.entity_id,
                "activity": r.activity,
                "timestamp": r.timestamp.isoformat() if r.timestamp else None,
                "ingested_at": r.ingested_at.isoformat() if r.ingested_at else None,
                "data_quality": r.data_quality,
            }
            for r in rows
        ],
    }


@router.get("/{event_id}")
def get_event(event_id: uuid.UUID, db: Session = Depends(get_db)):
    row = db.execute(text("SELECT * FROM events WHERE event_id = :eid"), {"eid": event_id}).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Evento no encontrado")
    return {
        "event_id": str(row.event_id),
        "domain": row.domain,
        "case_id": row.case_id,
        "entity_type": row.entity_type,
        "entity_id": row.entity_id,
        "activity": row.activity,
        "timestamp": row.timestamp.isoformat() if row.timestamp else None,
        "ingested_at": row.ingested_at.isoformat() if row.ingested_at else None,
        "attributes": row.attributes,
        "previous_state": row.previous_state,
        "current_state": row.current_state,
        "data_quality": row.data_quality,
    }
