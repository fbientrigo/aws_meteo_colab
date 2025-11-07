# `aws_meteo_colab.xarray_utils`

Utilidades comunes para trabajar con objetos de `xarray` dentro de los
notebooks y pipelines del proyecto.

## Funciones disponibles

- `_pick_var(ds, candidates=None)`: Selecciona la primera variable disponible en
  `ds` según una lista de preferencia opcional.
- `_ensure_celsius(da)`: Convierte series en Kelvin a grados Celsius manteniendo
  los metadatos originales.
- `_pick_point_coords(ds, prefer_lat=-33.45, prefer_lon=-70.65)`: Determina las
  coordenadas más cercanas a un punto de interés, devolviendo nombre y valores
  de las coordenadas.
- `_assert_dims(da, required=("time", "latitude", "longitude"))`: Verifica que
  un `DataArray` contenga las dimensiones esperadas, levantando un `ValueError`
  en caso contrario.
- `_shape_info(tag, obj)`: Imprime en consola un resumen de dimensiones o shape
  de un objeto para depuración rápida.

## Ejemplos de uso

```python
import xarray as xr
from aws_meteo_colab.xarray_utils import (
    _assert_dims,
    _ensure_celsius,
    _pick_point_coords,
    _pick_var,
)

# Dataset ficticio
ds = xr.Dataset({
    "t2m": ("time", [270.0, 271.5]),
}, coords={"time": ["2020-01", "2020-02"], "latitude": ("latitude", [-34.0]), "longitude": ("longitude", [-71.0])})

var_name = _pick_var(ds)
da = _ensure_celsius(ds[var_name])
_assert_dims(da.expand_dims(latitude=[-34.0], longitude=[-71.0]))
lat_name, lon_name, lat_val, lon_val = _pick_point_coords(ds)
print(f"Seleccionado {var_name} en ({lat_name}={lat_val}, {lon_name}={lon_val})")
```

Estos helpers se diseñaron para reutilizarse en notebooks, scripts y pruebas
unitarias.
