# `aws_meteo_colab.indices.maps`

Rutinas de trazado y agregación espacial utilizadas por los notebooks de
cálculo de índices climáticos.

## API

- `to_2d_month_slice(da, month_target)`: extrae un `DataArray` 2D (`latitude`,
  `longitude`) para el mes indicado, interpretando automáticamente coordenadas
  `time` o `month`.
- `imshow_map(ax, da, title, vlim=3.0, cmap="RdBu_r")`: renderiza un mapa con
  `matplotlib`, fijando límites simétricos y agregando una barra de color.
- `area_mean_weighted(da, month_target)`: calcula el promedio espacial ponderado
  por coseno de la latitud para el mes solicitado.

## Ejemplo de integración

```python
import matplotlib.pyplot as plt
import xarray as xr
from aws_meteo_colab.indices.maps import (
    area_mean_weighted,
    imshow_map,
    to_2d_month_slice,
)

da = xr.tutorial.load_dataset("air_temperature")["air"].sel(time=slice("2013-01", "2013-03"))
month_slice = to_2d_month_slice(da, "2013-02")

fig, ax = plt.subplots(figsize=(6, 4))
imshow_map(ax, month_slice, "Anomalía febrero 2013", vlim=10)
fig.tight_layout()

mean_val = area_mean_weighted(da, "2013-02")
print(f"Promedio ponderado febrero: {mean_val:.2f} {da.attrs.get('units', '')}")
```

La separación en módulos facilita reutilizar estos helpers desde scripts y
pruebas automáticas, manteniendo los notebooks enfocados en la explicación
conceptual.
