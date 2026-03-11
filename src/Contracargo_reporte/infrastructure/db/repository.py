import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2

from ...domain.cuid import fecha_a_cuid
from ..config import Settings


LOGGER = logging.getLogger(__name__)

# Rutas SQL por motor.
PG_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "TUTATI.sql"


class PostgresRepository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Carga SQL parametrizado para PostgreSQL.
        self._query = PG_QUERY_PATH.read_text(encoding="utf-8")
        # Carga SQL principal (TUTATI).
        # Otros SQLs no se usan en este proyecto.

    def ejecutar_consulta_sql(
        self,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        return self._ejecutar_sql(self._query, fecha_inicio, fecha_fin)

    def _ejecutar_sql(
        self,
        query: str,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        # Convierte rango fecha->CUID para filtro en PostgreSQL.
        inicio_dt = datetime.combine(fecha_inicio, datetime.min.time())
        fin_dt = datetime.combine(fecha_fin + timedelta(days=1), datetime.min.time())
        params = {
            "fecha1": fecha_a_cuid(inicio_dt),
            "fecha2": fecha_a_cuid(fin_dt),
        }

        # Reintenta la conexion ante cortes.
        for intento in range(1, self._settings.reintentos + 1):
            conn = None
            cur = None
            try:
                conn = psycopg2.connect(
                    host=self._settings.pg_host,
                    dbname=self._settings.pg_name,
                    user=self._settings.pg_user,
                    password=self._settings.pg_password,
                    port=self._settings.pg_port,
                    sslmode=self._settings.pg_sslmode,
                    connect_timeout=self._settings.pg_connect_timeout,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                conn.autocommit = True
                cur = conn.cursor()
                # Ejecuta SQL y devuelve filas + cabeceras.
                cur.execute(query, params)
                rows = cur.fetchall()
                if cur.description is None:
                    raise RuntimeError("La consulta PostgreSQL no devolvio metadatos de columnas.")
                cols = [c[0] for c in cur.description]
                return rows, cols
            except psycopg2.OperationalError as exc:
                # Log por intento para diagnostico de red.
                LOGGER.warning(
                    "Conexion PostgreSQL caida (intento %s/%s): %s",
                    intento,
                    self._settings.reintentos,
                    exc,
                )
                if intento == self._settings.reintentos:
                    raise
                time.sleep(self._settings.espera_segundos)
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

        raise RuntimeError("No se pudo ejecutar la consulta PostgreSQL tras todos los reintentos.")

    def probar_conexion(self) -> None:
        # Conexion corta para validar acceso a PostgreSQL.
        conn = psycopg2.connect(
            host=self._settings.pg_host,
            dbname=self._settings.pg_name,
            user=self._settings.pg_user,
            password=self._settings.pg_password,
            port=self._settings.pg_port,
            sslmode=self._settings.pg_sslmode,
            connect_timeout=self._settings.pg_connect_timeout,
        )
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchone()
        finally:
            cur.close()
            conn.close()
