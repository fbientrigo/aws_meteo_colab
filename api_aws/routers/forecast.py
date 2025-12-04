from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from lib.forecast.engine import forecast_damped_persistence
from lib.tests.conftest_climate import generate_mock_era5_monthly, generate_mock_climatology

router = APIRouter(prefix="/forecast", tags=["forecast"])

class ForecastRequest(BaseModel):
    latitude: float
    longitude: float

class ForecastStep(BaseModel):
    date: str
    mean: float
    p05: float
    p50: float
    p95: float

class ForecastResponse(BaseModel):
    history: List[Dict[str, Any]]
    forecast: List[ForecastStep]

from s3_helpers import list_runs, list_steps, load_dataset
# We keep s3_helpers imports to not break if we revert, but we won't use them for the primary flow
from lib.indices.construct import OUT_ALL, build_era5_t2m_monthly_chile
import os
import xarray as xr

# Instantiate mocks for Climatology ONLY (until we have real climatology)
_mock_clim_gen = generate_mock_climatology()
DATASET_CLIM = _mock_clim_gen()

def get_local_data():
    """
    Attempts to load the local ERA5 NetCDF file.
    Path: tmp/ERA5_T2M_monthly_1991_2025_chile.nc
    """
    if os.path.exists(OUT_ALL):
        return xr.open_dataset(OUT_ALL)
    return None

@router.post("/predict", response_model=ForecastResponse)
async def predict_forecast(request: ForecastRequest):
    """
    Generates a Damped Persistence forecast for a given location.
    Uses LOCAL CDSAPI DATA (if available) or MOCKS.
    """
    lat = request.latitude
    lon = request.longitude
    
    # 1. Try Get Real Data from Local File
    ds_local = get_local_data()
    
    if ds_local is not None:
        try:
            # Select nearest point
            # Note: ERA5 from CDSAPI might have 'latitude'/'longitude' or 'lat'/'lon'
            # The construct.py normalizes to 'latitude'/'longitude'
            ds_point = ds_local.sel(latitude=lat, longitude=lon, method="nearest")
            
            # Extract value (t2m)
            if "t2m" in ds_point:
                current_val = float(ds_point["t2m"].isel(time=-1).values)
                current_date = pd.Timestamp(ds_point["time"].isel(time=-1).values)
            else:
                # Fallback
                current_val = 288.0
                current_date = pd.Timestamp.now()
                
            ds_local.close()
            
        except Exception as e:
            # Log error and fall back to mock
            print(f"Error reading local data: {e}")
            ds_local.close()
            # Fallback to mock below
            ds_point = None
    else:
        ds_point = None

    # 2. Fallback to Mock if Local Data Missing or Failed
    if ds_point is None:
        # Fallback to Mock
        ds_point = DATASET_ERA5.sel(latitude=lat, longitude=lon, method="nearest")
        current_val = float(ds_point["t2m"].isel(time=-1).values)
        current_date = pd.Timestamp(ds_point["time"].isel(time=-1).values)

    # 3. Get Climatology (Mocked for now)
    try:
        clim_point = DATASET_CLIM.sel(latitude=lat, longitude=lon, method="nearest")
        clim_means = clim_point["mean"].values.tolist()
        clim_stds = clim_point["std"].values.tolist()
    except Exception:
        clim_means = [288.0] * 12
        clim_stds = [2.0] * 12
    
    # 4. Run Forecast Engine
    forecast_steps = forecast_damped_persistence(
        current_value=current_val,
        current_date=current_date,
        climatology_means=clim_means,
        climatology_stds=clim_stds,
        horizon_months=24
    )
    
    # 5. Format History
    history = [{
        "date": current_date.strftime("%Y-%m-%d"),
        "value": current_val
    }]
        
    return {
        "history": history,
        "forecast": forecast_steps
    }
