# main.py
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from s3_helpers import list_runs, list_steps, load_dataset

app = FastAPI(
    title="Pangu MVP STI API",
    description="API para servir índices STI desde NetCDF en S3",
    version="0.1.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/sti/runs")
def get_runs():
    runs = list_runs()
    return {"runs": runs}


@app.get("/sti/{run}/steps")
def get_steps(run: str):
    steps = list_steps(run)
    if not steps:
        raise HTTPException(status_code=404, detail=f"No se encontraron steps para run={run}")
    return {"run": run, "steps": steps}


@app.get("/sti/{run}/{step}/summary")
def get_summary(run: str, step: str):
    """
    Devuelve estadísticas básicas del dataset:
    - dimensiones
    - variables
    - min/max/mean de la variable 'sti'
    """
    try:
        ds = load_dataset(run, step)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="NetCDF no encontrado en S3")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error abriendo NetCDF: {e}")

    if "sti" not in ds.data_vars:
        ds.close()
        raise HTTPException(status_code=500, detail="Variable 'sti' no encontrada en el dataset")

    sti = ds["sti"]

    summary: Dict[str, Any] = {
        "run": run,
        "step": step,
        "dims": {k: int(v) for k, v in ds.dims.items()},
        "coords": list(ds.coords.keys()),
        "vars": list(ds.data_vars.keys()),
        "sti_stats": {
            "min": float(sti.min().values),
            "max": float(sti.max().values),
            "mean": float(sti.mean().values),
        },
    }
    ds.close()
    return JSONResponse(summary)


@app.get("/sti/{run}/{step}/subset")
def get_subset(
    run: str,
    step: str,
    lat_min: float = Query(..., description="Latitud mínima (grados)"),
    lat_max: float = Query(..., description="Latitud máxima (grados)"),
    lon_min: float = Query(..., description="Longitud mínima (grados)"),
    lon_max: float = Query(..., description="Longitud máxima (grados)"),
):
    """
    Devuelve un recorte geográfico de la variable 'sti' como JSON.
    Ojo con el tamaño: para MVP, usar bounding boxes razonables.
    """
    try:
        ds = load_dataset(run, step)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="NetCDF no encontrado en S3")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error abriendo NetCDF: {e}")

    if "sti" not in ds.data_vars:
        ds.close()
        raise HTTPException(status_code=500, detail="Variable 'sti' no encontrada en el dataset")

    # Asumimos esquema tipo ERA5 con coords "latitude" y "longitude".
    # Muchas veces latitude viene de 90 -> -90, por eso usamos slice(lat_max, lat_min).
    try:
        sub = ds["sti"].sel(
            latitude=slice(lat_max, lat_min),
            longitude=slice(lon_min, lon_max),
        )
    except Exception as e:
        ds.close()
        raise HTTPException(status_code=400, detail=f"Error en recorte lat/lon: {e}")

    data = sub.values.tolist()
    lats: List[float] = sub["latitude"].values.tolist()
    lons: List[float] = sub["longitude"].values.tolist()

    ds.close()
    return {
        "run": run,
        "step": step,
        "latitudes": lats,
        "longitudes": lons,
        "sti": data,
    }
