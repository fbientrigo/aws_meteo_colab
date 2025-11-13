# --- Celda 3.1: FIX de archivos (gzip/zip/tar/html) + verificación NetCDF ---
import os, gzip, zipfile, tarfile, shutil, netCDF4 as nc, pathlib
import xarray as xr

def _sniff(path):
    p = pathlib.Path(path)
    if not p.exists() or p.stat().st_size == 0: return "empty"
    head = open(path, "rb").read(512)
    if head.startswith(b"\x1f\x8b"): return "gzip"
    if head.startswith(b"PK\x03\x04"): return "zip"
    if head.startswith(b"\x89HDF\r\n\x1a\n"): return "netcdf4"
    if head.startswith(b"CDF"): return "netcdf3"
    if head.startswith(b"GRIB"): return "grib"
    if head.lstrip().lower().startswith(b"<html") or head.lstrip().lower().startswith(b"<!doctype html"):
        return "html"
    if b"ustar" in head[257:265]: return "tar"
    return "unknown"

def repair_era5_file_inplace(path: str) -> str:
    kind = _sniff(path); print(f"[repair] {path} → {kind}")
    if kind in {"netcdf3","netcdf4"}:
        nc.Dataset(path).close(); return path
    if kind == "empty": raise OSError("Archivo vacío, reintenta descarga.")
    if kind == "html": raise OSError("El archivo es HTML (error CDS). Revisa key/parámetros.")
    if kind == "grib": raise OSError("Archivo GRIB: pide 'format: netcdf' o abre con engine='cfgrib'.")

    tmp = path + ".tmp.nc"
    if kind == "gzip":
        with gzip.open(path, "rb") as fin, open(tmp, "wb") as fout: shutil.copyfileobj(fin, fout)
    elif kind == "zip":
        with zipfile.ZipFile(path) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith(".nc")]
            if not members: raise OSError("ZIP sin .nc")
            with zf.open(members[0]) as fin, open(tmp, "wb") as fout: shutil.copyfileobj(fin, fout)
    elif kind == "tar":
        with tarfile.open(path) as tf:
            members = [m for m in tf.getmembers() if m.isfile() and m.name.lower().endswith(".nc")]
            if not members: raise OSError("TAR sin .nc")
            with tf.extractfile(members[0]) as fin, open(tmp, "wb") as fout: shutil.copyfileobj(fin, fout)
    elif kind == "unknown":
        try:
            nc.Dataset(path).close(); return path
        except Exception as e:
            raise OSError(f"Formato desconocido y no abre: {e}")

    nc.Dataset(tmp).close()
    bak = path + ".bak"
    if os.path.exists(bak): os.remove(bak)
    os.rename(path, bak); os.replace(tmp, path)
    print("[repair] Reparado ✓ (backup .bak)")
    return path

def harmonize_era5(ds: xr.Dataset, *, is_pl: bool, enforce_shape: bool=True,
                   expected=(721,1440), select_expver=0, select_number=0,
                   target_vertical="pressure_level") -> xr.Dataset:
    out = ds
    rn = {}
    if "valid_time" in out.coords and "time" not in out.coords: rn["valid_time"]="time"
    if "forecast_time" in out.coords and "time" not in out.coords: rn["forecast_time"]="time"
    if rn: out = out.rename(rn)

    if is_pl:
        if target_vertical not in out.coords:
            if "pressure_level" in out.coords: out = out.rename({"pressure_level":target_vertical})
            elif "level" in out.coords:       out = out.rename({"level":target_vertical})

    if "expver" in out.sizes and select_expver is not None:
        out = out.isel(expver=select_expver).drop_vars("expver", errors="ignore")
    if "number" in out.sizes and select_number is not None:
        out = out.isel(number=select_number).drop_vars("number", errors="ignore")

    if "latitude" in out.coords:
        lat = out.latitude
        if float(lat[0]) < float(lat[-1]):
            out = out.reindex(latitude=lat[::-1])

    if "longitude" in out.coords:
        lon = out.longitude
        if float(lon.min()) < 0: out = out.assign_coords(longitude=(lon % 360))
        out = out.sortby("longitude")

    if enforce_shape and all(k in out.coords for k in ("latitude","longitude")):
        if (out.sizes["latitude"], out.sizes["longitude"]) != expected:
            raise ValueError(f"Malla no es {expected}; obtuviste {(out.sizes['latitude'], out.sizes['longitude'])}")
    return out
