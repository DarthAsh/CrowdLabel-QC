"""Convenience package shim for development without installing the project.

This module makes ``import qcc`` work directly from a source checkout by
redirecting to the real implementation that lives under ``src/qcc``.
When the project is installed as a package this shim is not part of the
wheel and the regular package is used instead.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sys

_PACKAGE_NAME = __name__
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
_SRC_PACKAGE = _PROJECT_ROOT / "src" / _PACKAGE_NAME

if str(_SRC_PACKAGE) not in sys.path:
    # Ensure the loader can resolve submodules like ``qcc.cli``.
    sys.path.insert(0, str(_SRC_PACKAGE.parent))

_spec = importlib.util.spec_from_file_location(
    _PACKAGE_NAME,
    _SRC_PACKAGE / "__init__.py",
    submodule_search_locations=[str(_SRC_PACKAGE)],
)
if _spec is None or _spec.loader is None:
    raise ImportError("Cannot load embedded qcc package")

_module = importlib.util.module_from_spec(_spec)
# Register the module before executing so intra-package imports resolve.
sys.modules[_PACKAGE_NAME] = _module
_assert_loader = _spec.loader
_assert_loader.exec_module(_module)

# Replace the shim's module attributes with the real implementation.
globals().update(_module.__dict__)
