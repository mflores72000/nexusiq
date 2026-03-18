import uuid
from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import get_db, SessionLocal
from src.pipeline.ingest import run_ingestion
from src.twins.twin_service import materialize_twins
from src.correlations.engine import run_correlation_engine

router_correlations = APIRouter(prefix="/correlations", tags=["Correlations"])
router_pipeline = APIRouter(prefix="/pipeline", tags=["Pipeline"])


@router_correlations.get("/")
def get_correlations(db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT event_id, timestamp, attributes FROM events WHERE domain = 'INSIGHT' AND activity = 'correlation_found' ORDER BY timestamp DESC")
    ).fetchall()

    if not rows:
        return {"message": "Sin correlaciones aún. Ejecutar POST /pipeline/run primero.", "correlations": []}

    return {
        "total": len(rows),
        "correlations": [
            {"event_id": str(r.event_id), "computed_at": r.timestamp.isoformat() if r.timestamp else None, **r.attributes}
            for r in rows
        ],
    }


@router_correlations.post("/run")
def run_correlations_now(db: Session = Depends(get_db)):
    results = run_correlation_engine(db)
    return {"message": f"{len(results)} correlaciones significativas encontradas.", "count": len(results)}


@router_pipeline.post("/run")
def trigger_pipeline(background_tasks: BackgroundTasks):
    """Dispara la ingesta en background. Ver progreso en GET /events?domain=SYSTEM."""
    job_id = str(uuid.uuid4())

    def _run():
        import logging
        log = logging.getLogger(__name__)
        try:
            run_ingestion()
            db = SessionLocal()
            try:
                materialize_twins(db)
                run_correlation_engine(db)
            finally:
                db.close()
        except Exception as exc:
            log.exception("Pipeline job %s falló: %s", job_id, exc)

    background_tasks.add_task(_run)
    return {"job_id": job_id, "status": "started", "message": "Ver progreso en GET /events?domain=SYSTEM"}
