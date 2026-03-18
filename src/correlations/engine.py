import logging
import uuid
from datetime import datetime, timezone
from itertools import combinations
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from sklearn.metrics import mutual_info_score
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.database import SessionLocal
from src.models import Event

logger = logging.getLogger(__name__)

NUMERIC_COLS = [
    "Air temperature [K]", "Process temperature [K]", "Rotational speed [rpm]",
    "Torque [Nm]", "Tool wear [min]", "Machine failure",
]
CORR_THRESHOLD = 0.5
P_THRESHOLD = 0.05


def _load_source_events_as_dataframe(db: Session) -> pd.DataFrame:
    """Carga eventos SOURCE del log como DataFrame. Solo lee del event log."""
    rows = db.execute(
        text("""
            SELECT case_id, entity_id AS machine_type, timestamp, attributes
            FROM events
            WHERE domain = 'SOURCE' AND entity_type = 'machine'
            ORDER BY timestamp ASC
        """)
    ).fetchall()

    if not rows:
        return pd.DataFrame()

    records = []
    for row in rows:
        attrs = row.attributes or {}
        record = {"case_id": row.case_id, "machine_type": row.machine_type, "timestamp": row.timestamp}
        for col in NUMERIC_COLS:
            val = attrs.get(col)
            record[col] = float(val) if val is not None else np.nan
        records.append(record)

    return pd.DataFrame(records)


def _detrend_series(series: pd.Series) -> pd.Series:
    """Elimina tendencia lineal. Detecta correlaciones espurias por tendencia temporal común."""
    valid = series.dropna()
    if len(valid) < 10:
        return series
    slope, intercept, *_ = scipy_stats.linregress(np.arange(len(valid)), valid.values)
    return series - (slope * np.arange(len(series)) + intercept)


def _generate_narrative(var1: str, var2: str, pearson_r: float, spearman_r: float,
                        mi: float, p_value: float, is_spurious: bool) -> str:
    direction = "positiva" if pearson_r > 0 else "negativa"
    strength = "muy fuerte" if abs(pearson_r) > 0.8 else "fuerte" if abs(pearson_r) > 0.6 else "moderada"
    spurious_note = (
        " ⚠️ ADVERTENCIA: Esta correlación podría ser espuria — desaparece al eliminar la tendencia temporal."
        if is_spurious else ""
    )
    return (
        f"'{var1}' y '{var2}' muestran una correlación {direction} {strength} "
        f"(Pearson r={pearson_r:.3f}, Spearman ρ={spearman_r:.3f}, "
        f"Información Mutua={mi:.3f}, p={p_value:.4f}). "
        f"Existe una relación estadísticamente significativa entre estas variables en el proceso de manufactura.{spurious_note}"
    )


def run_correlation_engine(db: Session | None = None) -> list[dict[str, Any]]:
    """
    Calcula correlaciones (Pearson, Spearman, MI) sobre datos del event log.
    Aplica corrección de Bonferroni y detrending. Guarda resultados como eventos INSIGHT.
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    results = []

    try:
        df = _load_source_events_as_dataframe(db)
        if df.empty:
            logger.warning("No hay eventos SOURCE. Ejecutar ingesta primero.")
            return []

        logger.info("Datos cargados: %d filas", len(df))

        detrended = {col: _detrend_series(df[col]) for col in NUMERIC_COLS if col in df.columns}

        p_values_raw = []
        pair_stats = []

        for var1, var2 in combinations(NUMERIC_COLS, 2):
            s1 = df[var1].dropna()
            s2 = df[var2].dropna()
            common_idx = s1.index.intersection(s2.index)
            s1, s2 = s1[common_idx], s2[common_idx]

            if len(s1) < 30:
                continue

            pearson_r, pearson_p = scipy_stats.pearsonr(s1, s2)
            spearman_r, _ = scipy_stats.spearmanr(s1, s2)

            try:
                mi = mutual_info_score(
                    pd.cut(s1, bins=10, labels=False).fillna(0).astype(int),
                    pd.cut(s2, bins=10, labels=False).fillna(0).astype(int),
                )
            except Exception:
                mi = 0.0

            try:
                detrended_r, _ = scipy_stats.pearsonr(detrended.get(var1, s1)[common_idx].dropna(),
                                                       detrended.get(var2, s2)[common_idx].dropna())
                is_spurious = abs(pearson_r) >= CORR_THRESHOLD and abs(detrended_r) < CORR_THRESHOLD * 0.5
            except Exception:
                is_spurious = False
                detrended_r = pearson_r

            p_values_raw.append(pearson_p)
            pair_stats.append({
                "var1": var1, "var2": var2,
                "pearson_r": float(pearson_r), "pearson_p": float(pearson_p),
                "spearman_r": float(spearman_r), "mi": float(mi),
                "is_spurious": is_spurious, "detrended_r": float(detrended_r),
                "n_samples": len(s1),
            })

        if not pair_stats:
            return []

        n_tests = len(p_values_raw)
        adjusted_p_values = [min(p * n_tests, 1.0) for p in p_values_raw]

        now = datetime.now(timezone.utc)
        for stat, p_adj in zip(pair_stats, adjusted_p_values):
            stat["p_adjusted"] = p_adj
            if abs(stat["pearson_r"]) >= CORR_THRESHOLD and p_adj < P_THRESHOLD:
                stat["narrative"] = _generate_narrative(
                    stat["var1"], stat["var2"], stat["pearson_r"],
                    stat["spearman_r"], stat["mi"], p_adj, stat["is_spurious"],
                )
                db.add(Event(
                    event_id=uuid.uuid4(),
                    domain="INSIGHT",
                    entity_type="pipeline",
                    entity_id="correlation-engine",
                    activity="correlation_found",
                    timestamp=now,
                    attributes=stat,
                    data_quality=1.0,
                ))
                results.append(stat)

        db.commit()
        logger.info("✅ %d correlaciones guardadas en el log.", len(results))

    except Exception as exc:
        logger.exception("Error en correlation engine: %s", exc)
        db.rollback()
        raise
    finally:
        if close_db:
            db.close()

    return results
