# s3_helpers.py
from typing import List
import re

import boto3
import xarray as xr

BUCKET = "pangu-mvp-data"
BASE_PREFIX = "indices/sti/"
INDEX_NAME = "sti"
REGION_NAME = "chile"  # usado en el nombre del archivo

s3_client = boto3.client("s3")


def list_runs() -> List[str]:
    """
    Lista los 'run=YYYYMMDDHH' disponibles bajo indices/sti/.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    runs = set()

    for page in paginator.paginate(Bucket=BUCKET, Prefix=BASE_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]  # ej: indices/sti/run=2025111500/step=072/sti_chile_run=...
            m = re.search(r"run=(\d{10})/", key)
            if m:
                runs.add(m.group(1))

    return sorted(runs)


def list_steps(run: str) -> List[str]:
    """
    Lista los 'step=XXX' disponibles para un run dado.
    """
    prefix = f"{BASE_PREFIX}run={run}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    steps = set()

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]  # ej: indices/sti/run=2025111500/step=072/...
            m = re.search(r"step=(\d{3})/", key)
            if m:
                steps.add(m.group(1))

    return sorted(steps)


def build_nc_key(run: str, step: str) -> str:
    """
    Construye el key del NetCDF, según convención:
    indices/sti/run=YYYYMMDDHH/step=XXX/sti_chile_run=YYYYMMDDHH_step=XXX.nc
    """
    filename = f"{INDEX_NAME}_{REGION_NAME}_run={run}_step={step}.nc"
    key = f"{BASE_PREFIX}run={run}/step={step}/{filename}"
    return key


def build_nc_s3_uri(run: str, step: str) -> str:
    """
    Ruta 's3://bucket/...' para xarray.
    """
    key = build_nc_key(run, step)
    return f"s3://{BUCKET}/{key}"


def load_dataset(run: str, step: str) -> xr.Dataset:
    """
    Abre el Dataset directamente desde S3 usando s3fs a través de xarray.
    """
    uri = build_nc_s3_uri(run, step)
    # xarray detecta 's3://' y usa s3fs. El rol IAM provee credenciales.
    ds = xr.open_dataset(uri)
    return ds
