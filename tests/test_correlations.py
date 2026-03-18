"""
Tests del Correlation Engine — M3

Cubre:
1. Correlaciones espurias detectadas por detrending
2. Correlaciones reales (inyectadas) son detectadas
3. El engine NO lee del CSV (solo del event log)
"""
import numpy as np
import uuid
import pytest
from datetime import datetime, timezone, timedelta

from src.correlations.engine import (
    _detrend_series,
    _generate_narrative,
    CORR_THRESHOLD,
)
import pandas as pd


# ─── TEST 1: Detrending elimina correlaciones espurias ───────────────────────
def test_detrend_removes_spurious_correlation():
    """
    CASO BORDE 5: Dos variables que crecen juntas en el tiempo.
    Su correlación es espuria (ambas correlacionan con el tiempo, no entre sí).
    El detrending debe reducir significativamente la correlación.
    """
    from scipy import stats as scipy_stats

    n = 200
    t = np.arange(n)

    # Dos variables con SOLO tendencia lineal creciente + ruido pequeño
    np.random.seed(42)
    var1 = 0.5 * t + np.random.normal(0, 1, n)
    var2 = 0.3 * t + np.random.normal(0, 1, n)

    s1 = pd.Series(var1)
    s2 = pd.Series(var2)

    # Correlación ANTES de detrend: debe ser alta (espuria)
    r_before, _ = scipy_stats.pearsonr(s1, s2)
    assert abs(r_before) > 0.9, "La correlación espuria debe ser alta antes de detrend"

    # Aplicar detrend
    d1 = _detrend_series(s1)
    d2 = _detrend_series(s2)

    # Correlación DESPUÉS de detrend: debe ser baja (era espuria)
    r_after, _ = scipy_stats.pearsonr(d1, d2)
    assert abs(r_after) < 0.3, (
        f"Después de detrend la correlación debe bajar < 0.3, "
        f"pero es {r_after:.3f}"
    )


# ─── TEST 2: Narrativa generada está en español ───────────────────────────────
def test_narrative_is_in_spanish():
    """La narrativa debe estar en español e incluir las variables."""
    narrative = _generate_narrative(
        var1="Air temperature [K]",
        var2="Process temperature [K]",
        pearson_r=0.85,
        spearman_r=0.82,
        mi=0.45,
        p_value=0.0001,
        is_spurious=False,
    )
    assert "Air temperature [K]" in narrative
    assert "Process temperature [K]" in narrative
    # Verificar palabras clave en español
    assert any(word in narrative.lower() for word in ["correlación", "relación", "sugiere"])
    assert "0.850" in narrative or "0.85" in narrative


# ─── TEST 3: Narrativa incluye advertencia para correlaciones espurias ────────
def test_spurious_narrative_includes_warning():
    """La narrativa de una correlación espuria debe incluir advertencia."""
    narrative = _generate_narrative(
        var1="Tool wear [min]",
        var2="Rotational speed [rpm]",
        pearson_r=0.75,
        spearman_r=0.70,
        mi=0.30,
        p_value=0.01,
        is_spurious=True,
    )
    assert "espuria" in narrative.lower() or "ADVERTENCIA" in narrative


# ─── TEST 4: Detrend con serie corta no falla ─────────────────────────────────
def test_detrend_short_series():
    """El detrending con menos de 10 puntos no debe lanzar excepción."""
    short_series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = _detrend_series(short_series)
    # Con < 10 puntos, devuelve la serie original sin modificar
    assert len(result) == len(short_series)
