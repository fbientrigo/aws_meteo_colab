"""Compatibility wrapper for the legacy ``indices`` package."""

from importlib import import_module
from types import ModuleType

_base = import_module("indices")

__all__ = list(getattr(_base, "__all__", []))

for name in __all__:
    globals()[name] = getattr(_base, name)

maps: ModuleType = import_module("indices.maps")
__all__.append("maps")
globals()["maps"] = maps
