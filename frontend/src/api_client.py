import requests
import streamlit as st
import pandas as pd
from typing import List, Dict, Any, Optional
from src.config import Config

class MeteoAPI:
    def __init__(self):
        self.base_url = Config.API_BASE_URL

    @st.cache_data(ttl=300)
    def get_available_runs(_self) -> List[str]:
        """Fetch available runs from the API."""
        try:
            response = requests.get(f"{_self.base_url}/sti/runs", timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("runs", [])
        except requests.RequestException as e:
            st.error(f"Error fetching runs: {e}")
            return []

    @st.cache_data(ttl=300)
    def get_steps_for_run(_self, run_id: str) -> List[str]:
        """Fetch available steps for a specific run."""
        try:
            response = requests.get(f"{_self.base_url}/sti/{run_id}/steps", timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("steps", [])
        except requests.RequestException as e:
            st.error(f"Error fetching steps for run {run_id}: {e}")
            return []

    @st.cache_data(ttl=300)
    def get_spatial_data(_self, run_id: str, step_id: str, 
                         lat_min: float = -56.0, lat_max: float = -17.0, 
                         lon_min: float = -75.0, lon_max: float = -66.0) -> Optional[pd.DataFrame]:
        """
        Fetch spatial data subset and convert to DataFrame.
        Default bbox matches Chile region from data.yaml.
        """
        params = {
            "lat_min": lat_min,
            "lat_max": lat_max,
            "lon_min": lon_min,
            "lon_max": lon_max
        }
        try:
            response = requests.get(f"{_self.base_url}/sti/{run_id}/{step_id}/subset", params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Reconstruct DataFrame
            # The API returns lists: latitudes, longitudes, and a 2D matrix 'sti'
            lats = data.get("latitudes", [])
            lons = data.get("longitudes", [])
            sti_matrix = data.get("sti", [])

            if not lats or not lons or not sti_matrix:
                st.warning("Received empty data from API.")
                return None

            # Flatten the data for PyDeck/Pandas
            # We need a list of dicts or a DataFrame with columns: lat, lon, sti
            rows = []
            for i, lat in enumerate(lats):
                for j, lon in enumerate(lons):
                    # sti_matrix is likely [lat_idx][lon_idx] or similar, need to verify orientation
                    # Assuming standard (lat, lon) indexing from xarray/numpy tolist()
                    val = sti_matrix[i][j]
                    rows.append({"lat": lat, "lon": lon, "sti": val})
            
            return pd.DataFrame(rows)

        except requests.RequestException as e:
            st.error(f"Error fetching spatial data: {e}")
            return None
        except Exception as e:
            st.error(f"Error processing data: {e}")
            return None
