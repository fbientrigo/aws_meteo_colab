# aws_meteo_colab

notebooks/ – pruebas y ejemplos.

models/ – wrappers para GraphCast, FourCastNet, GAIA.

utils/ – utilidades de descarga, reproyección, métricas.

data/ – caché local (se excluye del Git).

docs/ – documentación.

## Pangu

El paquete `aws_meteo_colab.pangu` concentra las utilidades del pipeline de Pangu
para armonizar ERA5, construir entradas y ejecutar inferencias. Ejemplo de uso:

```python
from aws_meteo_colab.pangu import (
    LEVELS_ORDER,
    harmonize_era5,
    load_era5_for_pangu,
    make_pangu_inputs,
    run_pangu_once,
)
```

Los notebooks existentes importan estas funciones directamente desde el paquete
para mantener una única fuente de verdad.

