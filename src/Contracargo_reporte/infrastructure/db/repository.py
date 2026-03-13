import logging
import time
from decimal import Decimal, InvalidOperation
from datetime import date
from pathlib import Path
from typing import Any

import psycopg2

from ..config import Settings


LOGGER = logging.getLogger(__name__)

# Rutas SQL por motor.
PG_RMA_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "RmaxOrder.sql"
PG_TIPO_ENTREGA_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "TipoEntrega.sql"
PG_TIPO_ENTREGA_FALLBACK_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "TipoEntregaFallback.sql"
PG_DNI_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "DniPorOrden.sql"
PG_EGIFT_STATUS_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "EgiftcardsStatusVale.sql"


class PostgresRepository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._query_rma = PG_RMA_QUERY_PATH.read_text(encoding="utf-8")
        self._query_tipo_entrega = PG_TIPO_ENTREGA_QUERY_PATH.read_text(encoding="utf-8")
        self._query_tipo_entrega_fallback = PG_TIPO_ENTREGA_FALLBACK_QUERY_PATH.read_text(encoding="utf-8")
        self._query_dni = PG_DNI_QUERY_PATH.read_text(encoding="utf-8")
        self._query_egift_status = PG_EGIFT_STATUS_QUERY_PATH.read_text(encoding="utf-8")

    def obtener_rmas_por_ordenes(self, ordenes: list[str]) -> dict[str, str]:
        if not ordenes:
            return {}
        orders_in = _render_in_list(ordenes)
        sql = self._query_rma.replace("{{orders_in}}", orders_in)
        rows, _cols = self._ejecutar_sql_raw(sql)
        LOGGER.info("RMA query rows: %s", len(rows))
        if rows:
            LOGGER.info("RMA sample rows: %s", rows[:5])
        resultado: dict[str, str] = {}
        for row in rows:
            if not row:
                continue
            uid_order = str(row[0]).strip() if row[0] is not None else ""
            total_order = row[1] if len(row) > 1 else None
            if len(row) >= 4:
                uid_rma = str(row[2]).strip() if row[2] is not None else ""
                total_rma = row[3]
            else:
                uid_rma = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
                total_rma = None
            if uid_order:
                if uid_rma:
                    # Si ya existe un valor no vacio, no lo sobreescribimos.
                    if uid_order not in resultado or not resultado[uid_order]:
                        resultado[uid_order] = uid_rma
                else:
                    # Solo registra vacio si no habia nada.
                    resultado.setdefault(uid_order, "")
        return resultado

    def obtener_rmas_totales_por_ordenes(self, ordenes: list[str]) -> tuple[list[tuple[Any, ...]], list[str]]:
        if not ordenes:
            return [], ["uid_orders", "total_order", "uid_rmas", "total_rma"]
        orders_in = _render_in_list(ordenes)
        sql = self._query_rma.replace("{{orders_in}}", orders_in)
        return self._ejecutar_sql_raw(sql)

    def obtener_dni_por_ordenes(self, ordenes: list[str]) -> dict[str, str]:
        if not ordenes:
            return {}
        orders_in = _render_in_list(ordenes)
        sql = self._query_dni.replace("{{orders_in}}", orders_in)
        rows, _cols = self._ejecutar_sql_raw(sql)
        dni_map: dict[str, str] = {}
        for row in rows:
            if not row:
                continue
            uid_order = str(row[0]).strip() if row[0] is not None else ""
            document = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
            if uid_order:
                dni_map[uid_order] = document
        return dni_map

    def obtener_egift_status_por_ordenes(self, ordenes: list[str]) -> dict[str, str]:
        if not ordenes:
            return {}
        orders_in = _render_in_list(ordenes)
        sql = self._query_egift_status.replace("{{orders_in}}", orders_in)
        rows, _cols = self._ejecutar_sql_raw(sql)
        status_map: dict[str, str] = {}
        for row in rows:
            if not row:
                continue
            uid_order = str(row[0]).strip() if row[0] is not None else ""
            status = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
            if uid_order:
                status_map[uid_order] = status
        return status_map

    def obtener_tipo_entrega_por_ordenes(
        self,
        ordenes: list[str],
    ) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
        if not ordenes:
            return {}, {}, {}
        orders_in = _render_in_list(ordenes)
        sql = self._query_tipo_entrega.replace("{{orders_in}}", orders_in)
        rows, _cols = self._ejecutar_sql_raw(sql)
        tipo_map: dict[str, str] = {}
        tienda_map: dict[str, str] = {}
        ubigeo_map: dict[str, str] = {}
        for row in rows:
            if not row:
                continue
            uid_order = str(row[0]) if row[0] is not None else ""
            tipo = str(row[1]) if row[1] is not None else ""
            if uid_order:
                tipo_map[uid_order] = tipo
                if len(row) > 2 and row[2] is not None:
                    tienda_map[uid_order] = str(row[2])
                if len(row) > 3 and row[3] is not None:
                    ubigeo_map[uid_order] = str(row[3])
        return tipo_map, tienda_map, ubigeo_map

    def obtener_tipo_entrega_fallback(
        self,
        ordenes: list[str],
    ) -> tuple[dict[str, str], dict[str, str]]:
        if not ordenes:
            return {}, {}
        orders_in = _render_in_list(ordenes)
        sql = self._query_tipo_entrega_fallback.replace("{{orders_in}}", orders_in)
        rows, _cols = self._ejecutar_sql_raw(sql)
        tipo_map: dict[str, str] = {}
        ubigeo_map: dict[str, str] = {}
        for row in rows:
            if not row:
                continue
            uid_order = str(row[0]) if row[0] is not None else ""
            tipo = str(row[1]) if row[1] is not None else ""
            if uid_order:
                tipo_map[uid_order] = tipo
                if len(row) > 2 and row[2] is not None:
                    ubigeo_map[uid_order] = str(row[2])
        return tipo_map, ubigeo_map

    def _ejecutar_sql_params(
        self,
        query: str,
        params: dict[str, Any],
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
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
                cur.execute(query, params)
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description] if cur.description else []
                return rows, cols
            except psycopg2.OperationalError as exc:
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

    def _ejecutar_sql_raw(self, query: str) -> tuple[list[tuple[Any, ...]], list[str]]:
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
                cur.execute(query)
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description] if cur.description else []
                return rows, cols
            except psycopg2.OperationalError as exc:
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


def _totales_diferentes(a: Any, b: Any) -> bool:
    try:
        da = _parse_decimal(a)
        db = _parse_decimal(b)
    except InvalidOperation:
        return False
    # Compara con 2 decimales (monto monetario)
    return da.quantize(Decimal("0.01")) != db.quantize(Decimal("0.01"))


def _parse_decimal(value: Any) -> Decimal:
    if value is None:
        raise InvalidOperation
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if text == "":
        raise InvalidOperation
    # Deja solo digitos, punto, coma y signo.
    cleaned = []
    for ch in text:
        if ch.isdigit() or ch in {".", ",", "-"}:
            cleaned.append(ch)
    text = "".join(cleaned)
    # Si tiene coma y punto, asume que la coma es miles.
    if "," in text and "." in text:
        text = text.replace(",", "")
    # Si solo tiene coma, asume coma decimal.
    elif "," in text and "." not in text:
        text = text.replace(",", ".")
    return Decimal(text)

    def _ejecutar_sql_raw(self, query: str) -> tuple[list[tuple[Any, ...]], list[str]]:
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
                cur.execute(query)
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description] if cur.description else []
                return rows, cols
            except psycopg2.OperationalError as exc:
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


def _render_in_list(values: list[str]) -> str:
    # Renderiza lista para IN ('a','b','c') con escape basico.
    safe = []
    for value in values:
        raw = str(value).replace("'", "''")
        safe.append(f"'{raw}'")
    return ", ".join(safe)
