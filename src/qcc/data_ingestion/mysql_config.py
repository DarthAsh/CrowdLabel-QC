"""Configuration helpers for connecting to a MySQL database."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class MySQLConfig:
    """Connection parameters required for accessing a MySQL database.

    Parameters
    ----------
    host:
        Hostname or IP address of the MySQL server.
    user:
        Username used to authenticate with the database.
    password:
        Password used to authenticate with the database.
    database:
        Name of the database to connect to.
    port:
        TCP port where the MySQL server is listening. Defaults to 3306.
    charset:
        Optional character set to use for the connection. UTF-8 is used by
        default when not provided.
    use_pure:
        When ``True``, ``mysql.connector`` will use the Python implementation
        instead of the C extension. Defaults to ``False`` to automatically pick
        the optimal implementation.
    """

    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    charset: Optional[str] = None
    use_pure: bool = True

    @classmethod
    def from_env(cls, prefix: str = "MYSQL") -> "MySQLConfig":
        """Build a configuration object using environment variables.

        Parameters
        ----------
        prefix:
            Prefix used in the environment variable names. For example, with
            the default prefix of ``MYSQL`` the variables ``MYSQL_HOST``,
            ``MYSQL_USER`` and so on are read.

        Returns
        -------
        MySQLConfig
            A populated configuration object.

        Raises
        ------
        ValueError
            If any required variables are missing.
        """

        env_map = {
            "host": os.getenv(f"{prefix}_HOST"),
            "user": os.getenv(f"{prefix}_USER"),
            "password": os.getenv(f"{prefix}_PASSWORD"),
            "database": os.getenv(f"{prefix}_DATABASE"),
            "port": os.getenv(f"{prefix}_PORT"),
            "charset": os.getenv(f"{prefix}_CHARSET"),
            "use_pure": True,
        }

        missing = [key for key in ("host", "user", "password", "database") if not env_map[key]]
        if missing:
            raise ValueError(
                "Missing required environment variables for MySQL configuration: "
                + ", ".join(f"{prefix}_{name.upper()}" for name in missing)
            )

        port_value = int(env_map["port"]) if env_map["port"] else 3306
        use_pure_value = (
            env_map["use_pure"].strip().lower() in {"1", "true", "yes"}
            if env_map["use_pure"]
            else False
        )

        return cls(
            host=env_map["host"],
            user=env_map["user"],
            password=env_map["password"],
            database=env_map["database"],
            port=port_value,
            charset=env_map["charset"],
            use_pure=use_pure_value,
        )

    def as_connector_kwargs(self) -> Dict[str, object]:
        """Return keyword arguments compatible with ``mysql.connector.connect``."""

        kwargs: Dict[str, object] = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "port": self.port,
            "use_pure": self.use_pure,
        }
        if self.charset:
            kwargs["charset"] = self.charset
        return kwargs
