"""Connection to the project database (PostgreSQL + PostGIS, service project-db).

Settings come from the environment (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER,
POSTGRES_PASSWORD), filled from .env when run on the host. Inside the Docker network the
Airflow containers use host project-db and port 5432 (set in docker-compose.yml).
"""
from __future__ import annotations

import os

from ..config import _load_dotenv


def conn_params() -> dict:
    _load_dotenv()
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "volcano"),
        "user": os.getenv("POSTGRES_USER", "volcano"),
        "password": os.getenv("POSTGRES_PASSWORD", ""),
    }


def connect(**overrides):
    import psycopg   # imported here so modules that only build rows do not need the driver

    params = conn_params() | overrides
    return psycopg.connect(**params, application_name="volcano-loader", connect_timeout=10)


def describe() -> str:
    p = conn_params()
    return f"{p['user']}@{p['host']}:{p['port']}/{p['dbname']}"
