from datetime import datetime, timezone
from pathlib import Path
from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import get_db

router = APIRouter(tags=["Health"])


@router.get("/health/view", response_class=HTMLResponse)
def get_dashboard():
    template_path = Path(__file__).parent.parent / "templates" / "dashboard.html"
    return HTMLResponse(content=template_path.read_text(encoding="utf-8"))

@router.get("/health")
def get_health_stats(db: Session = Depends(get_db)):
    event_count = db.execute(text("SELECT COUNT(*) FROM events")).scalar() or 0

    avg_lag = db.execute(text("""
        SELECT AVG(EXTRACT(EPOCH FROM (ingested_at - timestamp)) * 1000)
        FROM events WHERE domain = 'SOURCE' AND ingested_at IS NOT NULL
    """)).scalar()

    last_run = db.execute(text("""
        SELECT timestamp FROM events
        WHERE domain = 'SYSTEM' AND activity = 'pipeline_completed'
        ORDER BY timestamp DESC LIMIT 1
    """)).scalar()

    stale_twins = db.execute(text("""
        SELECT COUNT(DISTINCT entity_id) FROM (
            SELECT entity_id, MAX(timestamp) AS last_ts FROM events
            WHERE domain = 'TWIN_STATE' GROUP BY entity_id
        ) t WHERE last_ts < NOW() - INTERVAL '1 hour'
    """)).scalar() or 0

    insight_count = db.execute(text("SELECT COUNT(*) FROM events WHERE domain = 'INSIGHT'")).scalar() or 0

    return {
        "status": "healthy" if event_count > 0 else "empty",
        "event_count": event_count,
        "avg_lag_ms": round(float(avg_lag), 2) if avg_lag else 0.0,
        "stale_twins": int(stale_twins),
        "last_run_at": last_run.isoformat() if last_run else None,
        "insight_count": insight_count,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


