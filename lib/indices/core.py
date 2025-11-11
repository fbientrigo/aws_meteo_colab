# indices/core.py
from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from .spei import load_or_prepare_spei_series

def _rolling_sum(s: pd.Series, window_days: int) -> pd.Series:
    return s.rolling(window_days, min_periods=window_days).sum()

def _to_standard_score(x: pd.Series) -> pd.Series:
    """Convierte a score ~N(0,1) vía CDF empírica + invNorm (probit)."""
    from scipy.stats import norm
    ranks = x.rank(method="average", pct=True)
    eps = 1e-6
    return pd.Series(norm.ppf(np.clip(ranks, eps, 1-eps)), index=x.index)

@dataclass
class IndicesConfig:
    spi_window_days: int = 90      # ~3 meses
    spei_window_days: int = 90
    sti_window_days: int = 30

def compute_spi(daily_prec_mm: pd.Series, window_days: int) -> pd.Series:
    acc = _rolling_sum(daily_prec_mm, window_days)
    return _to_standard_score(acc.dropna()).reindex(daily_prec_mm.index)

def thornthwaite_monthly_pet(temp_c_monthly: pd.Series, lat_deg: float) -> pd.Series:
    """PET mensual (Thornthwaite) simplificado. Entrada: T media mensual (°C)."""
    # Calcular índice de calor anual I y a
    tpos = temp_c_monthly.clip(lower=0)
    I = (tpos/5).pow(1.514).sum()
    a = (6.75e-7)*I**3 - (7.71e-5)*I**2 + (1.792e-2)*I + 0.49239
    # Duración día aprox. por lat (corrección estacional simple)
    daylen_factor = temp_c_monthly.index.month.map(
        lambda m: {12:0.9,1:0.9,2:1.0,3:1.1,4:1.2,5:1.3,6:1.35,7:1.35,8:1.25,9:1.15,10:1.0,11:0.95}.get(m,1.0)
    )
    pet_cm = 16 * (10*tpos/I).pow(a) * daylen_factor  # mm/mes aprox
    return pet_cm

def compute_spei(daily_prec_mm: pd.Series,
                 daily_temp_c: pd.Series,
                 lat_deg: float,
                 window_days: int) -> pd.Series:
    """SPEI ≈ (P - PET) estandarizado (PET mensual Thornthwaite -> diario por reparto)."""
    # Agregar a mensual
    Pm = daily_prec_mm.resample("MS").sum()
    Tm = daily_temp_c.resample("MS").mean()
    PETm = thornthwaite_monthly_pet(Tm, lat_deg)
    Dm = (Pm - PETm).dropna()
    # Expandir a diario con forward-fill por mes
    Dd = Dm.reindex(pd.date_range(Dm.index.min(), Dm.index.max(), freq="D")).ffill()
    acc = _rolling_sum(Dd, window_days)
    spei = _to_standard_score(acc.dropna())
    return spei.reindex(daily_prec_mm.index)

def compute_sti(daily_temp_c: pd.Series, window_days: int) -> pd.Series:
    Tavg = daily_temp_c.rolling(window_days, min_periods=window_days).mean()
    return _to_standard_score(Tavg.dropna()).reindex(daily_temp_c.index)


def index_bucket(daily_prec_mm: pd.Series,
                 daily_temp_c: pd.Series,
                 cfg: IndicesConfig,
                 lat_deg: float,
                 external_spei: bool = True,
                 spei_timescale_months: int = 12,
                 spei_bbox: tuple[float,float,float,float] | None = None) -> pd.DataFrame:
    """
    Si external_spei=True y hay spei{s} en ./data/spei/, usa esa serie (daily ffill) para SPEI.
    Si no, calcula SPEI con Thornthwaite.
    """
    spi = compute_spi(daily_prec_mm, cfg.spi_window_days)
    # SPEI
    if external_spei and spei_bbox is not None:
        try:
            res = load_or_prepare_spei_series(
                time_scale=spei_timescale_months,
                bbox=spei_bbox,
                cache_dir="./data",
                prefer_cache=True
            )
            spei_daily = res["daily"]
            # Alinear índices con tus series diarias
            spei = spei_daily.reindex(daily_prec_mm.index, method="ffill")
        except Exception:
          xarray_utils
        spei = compute_spei(daily_prec_mm, daily_temp_c, lat_deg, cfg.spei_window_days)

    sti = compute_sti(daily_temp_c, cfg.sti_window_days)
    return pd.DataFrame({"SPI": spi, "SPEI": spei, "STI": sti})

