import xarray as xr

def _open_sfc_var(grib_path: str, short_name: str) -> xr.Dataset:
    """
    Abre solo una variable de superficie desde GRIB, filtrando por shortName.

    Parameters
    ----------
    grib_path : str
        Ruta al archivo GRIB.
    short_name : str
        Nombre corto GRIB (msl, 10u, 10v, 2t, etc.)

    Returns
    -------
    xr.Dataset
        Dataset con esa variable y sus coords.
    """
    return xr.open_dataset(
        grib_path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {"shortName": short_name},
            "indexpath": "",  # evita problemas de índices viejos
        },
    )

def open_sfc_xarr(grib_path: str) -> xr.Dataset:
    """
    Applys a previous merge of data, as t2m its different in height
    """
    ds_msl = _open_sfc_var(grib_path, "msl")
    ds_u10 = _open_sfc_var(grib_path, "10u")
    ds_v10 = _open_sfc_var(grib_path, "10v")
    ds_t2m = _open_sfc_var(grib_path, "2t")

    # Nos quedamos solo con las vars que nos interesan y mergeamos
    ds_sfc = xr.merge(
        [
            ds_msl[["msl"]],
            ds_u10[["u10"]],
            ds_v10[["v10"]], #objetos a 10m sobre elsuelo
            ds_t2m[["t2m"]], # objeto a 2m sobre el suelo, requiere override
        ], compat='override'
    )

    return ds_sfc


# ----
def _open_pl_var(grib_path: str, short_name: str) -> xr.Dataset:
    """
    Abre solo una variable de niveles de presión desde GRIB, filtrando por shortName.
    """
    return xr.open_dataset(
        grib_path,
        engine="cfgrib",
        backend_kwargs={
            "filter_by_keys": {
                "shortName": short_name,
                # opcionalmente podemos fijar el tipo de nivel:
                # "typeOfLevel": "isobaricInhPa",
            },
            "indexpath": "",
        },
    )


def open_pl_xarr(grib_path: str) -> xr.Dataset:
    """
    Construye un Dataset con [z (gh en EMCWF), q, t, u, v] en niveles de presión
    a partir de un archivo GRIB de HRES.
    """
    ds_z = _open_pl_var(grib_path, "gh")
    print(ds_z)
    ds_q = _open_pl_var(grib_path, "q")
    ds_t = _open_pl_var(grib_path, "t")
    ds_u = _open_pl_var(grib_path, "u")
    ds_v = _open_pl_var(grib_path, "v")

    ds_pl = xr.merge(
        [
            ds_z[["gh"]],
            ds_q[["q"]],
            ds_t[["t"]],
            ds_u[["u"]],
            ds_v[["v"]],
        ],
        compat="override",
    )

    return ds_pl
