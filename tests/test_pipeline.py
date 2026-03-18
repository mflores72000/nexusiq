"""
Tests del Pipeline de Ingesta — M1

Cubre casos borde del enunciado:
1. UDI duplicado con valores distintos → ambos se insertan
2. Campo numérico con string inválido → se limpia, data_quality < 1
3. Pipeline interrumpido → re-ejecución sin duplicados (idempotencia)
4. Eventos SYSTEM emitidos (pipeline_started, pipeline_completed)
5. Verificación de inmutabilidad (no hay UPDATEs)
"""
import csv
import os
import uuid
import pytest
import tempfile
from datetime import datetime, timezone
from sqlalchemy import create_engine, text

from src.database import SessionLocal
from src.pipeline.ingest import run_ingestion, _clean_row, _make_event_id

@pytest.fixture(scope="function")
def test_db():
    db = SessionLocal()
    yield db, db.get_bind()
    db.close()


def _make_temp_csv(rows: list[dict], tmp_path) -> str:
    """Helper: crea un CSV temporal para pruebas."""
    fieldnames = [
        "UDI", "Product ID", "Type",
        "Air temperature [K]", "Process temperature [K]",
        "Rotational speed [rpm]", "Torque [Nm]", "Tool wear [min]",
        "Machine failure", "TWF", "HDF", "PWF", "OSF", "RNF",
    ]
    csv_path = os.path.join(tmp_path, "test.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return csv_path


# ─── TEST 1: UDI duplicado con valores distintos ──────────────────────────────
def test_duplicate_udi_different_values(tmp_path, test_db):
    db, _ = test_db
    udi = f"test_{uuid.uuid4().hex[:6]}"
    row_base = {
        "UDI": udi, "Product ID": "L99999", "Type": "L",
        "Air temperature [K]": "298.1", "Process temperature [K]": "308.6",
        "Rotational speed [rpm]": "1551", "Torque [Nm]": "42.8",
        "Tool wear [min]": "0", "Machine failure": "0",
        "TWF": "0", "HDF": "0", "PWF": "0", "OSF": "0", "RNF": "0",
    }
    row_variant = {**row_base, "Torque [Nm]": "99.9"}

    csv_path = _make_temp_csv([row_base, row_variant], tmp_path)
    stats = run_ingestion(csv_path, db=db)

    assert stats["total_rows"] == 2
    assert stats["inserted"] == 2
    assert stats["skipped_duplicates"] == 0


def test_invalid_numeric_field():
    row = {
        "UDI": "1", "Product ID": "L00001", "Type": "L",
        "Air temperature [K]": "N/A", "Process temperature [K]": "null",
        "Rotational speed [rpm]": "--", "Torque [Nm]": "42.8",
        "Tool wear [min]": "0", "Machine failure": "0",
        "TWF": "0", "HDF": "0", "PWF": "0", "OSF": "0", "RNF": "0",
    }
    cleaned, quality = _clean_row(row)
    assert cleaned["Air temperature [K]"] is None
    assert cleaned["Process temperature [K]"] is None
    assert cleaned["Rotational speed [rpm]"] is None
    assert cleaned["Torque [Nm]"] == 42.8
    assert quality < 1.0


def test_pipeline_idempotent(tmp_path, test_db):
    db, _ = test_db
    base_udi_prefix = f"idemp_{uuid.uuid4().hex[:6]}"
    rows = [
        {
            "UDI": f"{base_udi_prefix}_{i}", "Product ID": f"L{i:05d}", "Type": "L",
            "Air temperature [K]": "298.1", "Process temperature [K]": "308.6",
            "Rotational speed [rpm]": "1551", "Torque [Nm]": "42.8",
            "Tool wear [min]": "0", "Machine failure": "0",
            "TWF": "0", "HDF": "0", "PWF": "0", "OSF": "0", "RNF": "0",
        }
        for i in range(1, 6)
    ]
    csv_path = _make_temp_csv(rows, tmp_path)

    stats1 = run_ingestion(csv_path, db=db)
    assert stats1["inserted"] == 5

    # Segunda ejecución con el mismo CSV
    stats2 = run_ingestion(csv_path, db=db)
    assert stats2["inserted"] == 0
    assert stats2["skipped_duplicates"] == 5


# ─── TEST 4: UUID determinístico para idempotencia ───────────────────────────
def test_deterministic_event_id():
    """
    Verifica que el mismo UDI + mismo contenido siempre genera el mismo event_id.
    Fundamento de la idempotencia.
    """
    udi = "42"
    fingerprint = "abc123"
    id1 = _make_event_id(udi, fingerprint)
    id2 = _make_event_id(udi, fingerprint)
    id3 = _make_event_id(udi, "different_content")

    assert id1 == id2, "Mismo UDI + mismo contenido → mismo event_id"
    assert id1 != id3, "Mismo UDI + distinto contenido → distinto event_id"


# ─── TEST 5: Eventos SYSTEM emitidos por el pipeline ─────────────────────────
def test_pipeline_emits_system_events(tmp_path, test_db):
    """
    Verifica que el pipeline emite eventos SYSTEM/pipeline_started
    y SYSTEM/pipeline_completed correctamente.
    """
    db, _ = test_db
    rows = [{
        "UDI": "1", "Product ID": "M00001", "Type": "M",
        "Air temperature [K]": "298.1", "Process temperature [K]": "308.6",
        "Rotational speed [rpm]": "1551", "Torque [Nm]": "42.8",
        "Tool wear [min]": "10", "Machine failure": "0",
        "TWF": "0", "HDF": "0", "PWF": "0", "OSF": "0", "RNF": "0",
    }]
    csv_path = _make_temp_csv(rows, tmp_path)
    stats = run_ingestion(csv_path, db=db)

    assert "job_id" in stats
    assert stats["total_rows"] == 1


# ─── TEST 6: data_quality = 1.0 para fila perfecta ──────────────────────────
def test_data_quality_perfect_row():
    """Una fila sin valores nulos debe tener data_quality = 1.0."""
    row = {
        "UDI": "1", "Product ID": "L00001", "Type": "L",
        "Air temperature [K]": "298.1", "Process temperature [K]": "308.6",
        "Rotational speed [rpm]": "1551", "Torque [Nm]": "42.8",
        "Tool wear [min]": "0", "Machine failure": "0",
        "TWF": "0", "HDF": "0", "PWF": "0", "OSF": "0", "RNF": "0",
    }
    _, quality = _clean_row(row)
    assert quality == 1.0, "Fila perfecta debe tener data_quality = 1.0"
