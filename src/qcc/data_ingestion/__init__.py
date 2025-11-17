"""Utilities for importing quality control data from MySQL."""

from __future__ import annotations

from typing import Any, Sequence

from .mysql_config import MySQLConfig
from . import mysql_importer as _mysql_importer

# ``mysql_importer`` exposes a couple of helpers that are convenient to re-export
# from the package root.  Some downstream environments reported seeing
# ``ImportError`` failures when older checkouts that lacked
# ``import_tag_prompt_deployment_tables`` were on ``sys.path``.  Instead of
# failing during package import, degrade gracefully and surface a clearer error
# message when the legacy module is encountered.

TableImporter = _mysql_importer.TableImporter
mysql_connection = _mysql_importer.mysql_connection
DEFAULT_TAG_PROMPT_TABLES: Sequence[str] = getattr(
    _mysql_importer,
    "DEFAULT_TAG_PROMPT_TABLES",
    (
        "answer_tags",
        "answers",
    ),
)

if hasattr(_mysql_importer, "import_tag_prompt_deployment_tables"):
    import_tag_prompt_deployment_tables = _mysql_importer.import_tag_prompt_deployment_tables
else:  # pragma: no cover - only used with stale installations

    def import_tag_prompt_deployment_tables(*args: Any, **kwargs: Any) -> Any:
        raise ImportError(
            "import_tag_prompt_deployment_tables is unavailable. "
            "Update CrowdLabel-QC to a newer revision or import"
            " qcc.data_ingestion.mysql_importer directly."
        )


__all__ = [
    "MySQLConfig",
    "TableImporter",
    "mysql_connection",
    "DEFAULT_TAG_PROMPT_TABLES",
    "import_tag_prompt_deployment_tables",
]
