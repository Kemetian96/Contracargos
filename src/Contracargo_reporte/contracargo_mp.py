from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import re
import pandas as pd

from .infrastructure.config import Settings, load_settings
from .infrastructure.db.repository import PostgresRepository
from .infrastructure.export import exportar_pestana_texto


LOGGER = logging.getLogger(__name__)


SOURCE_SHEET_NAME = "Export after collection"
DATA_SHEET_NAME = "Data"
TUTATI_SHEET_NAME = "Ejemplo"

NEW_COLUMNS = [
    "ORDEN",
    "RMA",
    "TIPO DE FACTURADA",
    "LUGAR DE ENTREGA",
    "PROVINCIA",
    "ESTADO MP",
    "OBSERVACION MP",
    "OBSERVACION INTERNA",
    "TIPO-COMERCIO",
    "DOMINIO",
]

ORDEN_SOURCE_COLUMN = "Referencia externa de la transacción"
GAP_COLUMNS = 3
GAP_COLUMN_PREFIX = "__gap__"


DATE_COLUMNS = {
    "Fecha de creación",
    "Plazo de la documentación",
    "Fecha de creación de la transacción",
}

DROP_COLUMNS = {
    "ID",
    "Detalle del motivo",
    "Resolución aplicada a",
    "Dinero de la resolución bloqueado",
    "Flow",
    "ID de la transacción",
    "Tipo de transacción",
    "Estado de la transacción",
    "Marketplace de transacción",
    "ID de la orden",
    "ID de la orden del comercio",
    "ID de la campaña",
    "Nombre de la campaña",
    "Unidad",
    "Subunidad",
    "Franquicia",
    "Nombre del emisor",
    "BIN",
    "Últimos 4 dígitos",
    "ID de transferencia del banco pagador",
    "ID de usuario CUS",
    "Procesado por",
    "ID del producto",
    "ID del ítem",
}


@dataclass(frozen=True)
class ReporteMPPaths:
    origen_excel: Path
    salida_excel: Path


def generar_reporte_mp(
    paths: ReporteMPPaths,
    fecha_inicio: date,
    fecha_fin: date,
    settings: Settings | None = None,
) -> None:
    """
    Genera Reporte_Mp.xlsx con:
    - Hoja Data: transformacion del Excel fuente con texto (excepto fechas).
    - Hoja Ejemplo: salida de TUTATI.sql.
    """
    settings = settings or load_settings()

    df = _cargar_y_transformar(paths.origen_excel)
    data_cols = _render_headers(list(df.columns))
    data_rows = _df_to_rows(df, date_columns=DATE_COLUMNS)

    repo = PostgresRepository(settings)
    tutati_rows, tutati_cols = repo.ejecutar_consulta_sql(fecha_inicio, fecha_fin)
    tutati_rows = _rows_to_text(tutati_rows)

    exportar_pestana_texto(
        rows=data_rows,
        cols=data_cols,
        ruta=paths.salida_excel,
        sheet_name=DATA_SHEET_NAME,
        date_columns=DATE_COLUMNS,
        date_format="dd/mm/yyyy",
    )
    exportar_pestana_texto(
        rows=tutati_rows,
        cols=tutati_cols,
        ruta=paths.salida_excel,
        sheet_name=TUTATI_SHEET_NAME,
    )


def _cargar_y_transformar(origen_excel: Path) -> pd.DataFrame:
    # Lee sin encabezados y promueve primera fila como header.
    raw = _leer_excel(origen_excel, SOURCE_SHEET_NAME)
    if raw.empty:
        return pd.DataFrame()
    header = raw.iloc[0].tolist()
    header = [_normalize_header(value, idx) for idx, value in enumerate(header)]
    df = raw.iloc[1:].copy()
    df.columns = header

    # Tipos: fechas y monto.
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    if "Monto" in df.columns:
        df["Monto"] = pd.to_numeric(df["Monto"], errors="coerce")
        df = df[df["Monto"].fillna(0) != 0]
        df = df.sort_values(by="Monto", ascending=True)

    # Elimina columnas no requeridas.
    df = df.drop(columns=[c for c in DROP_COLUMNS if c in df.columns], errors="ignore")

    if "Fecha de creación" in df.columns:
        df = df.sort_values(by="Fecha de creación", ascending=True)

    df = df.reset_index(drop=True)
    df = _insertar_columnas_custom(df)
    return df


def _leer_excel(origen_excel: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(origen_excel, sheet_name=sheet_name, header=None)
    except Exception as exc:
        # Fallback para archivos con estilos corruptos en openpyxl.
        try:
            return pd.read_excel(origen_excel, sheet_name=sheet_name, header=None, engine="calamine")
        except Exception as inner_exc:
            raise RuntimeError(
                "No se pudo leer el Excel con openpyxl. "
                "Prueba instalar python-calamine: pip install python-calamine"
            ) from inner_exc


def _normalize_header(value: Any, idx: int) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return f"col_{idx + 1}"
    text = str(value).strip()
    if not text:
        return f"col_{idx + 1}"
    # Quita el sufijo en parentesis: "Campo (field)" -> "Campo"
    text = re.sub(r"\s*\([^)]*\)\s*$", "", text).strip()
    return text if text else f"col_{idx + 1}"


def _df_to_rows(df: pd.DataFrame, date_columns: set[str]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for _, row in df.iterrows():
        cells = []
        for col, value in row.items():
            if col in date_columns:
                cells.append(_to_date(value))
            else:
                cells.append(_to_text(value))
        rows.append(tuple(cells))
    return rows


def _insertar_columnas_custom(df: pd.DataFrame) -> pd.DataFrame:
    # Inserta columnas nuevas despues de las columnas existentes + 3 columnas vacias.
    cols = list(df.columns)
    insert_pos = len(cols)

    # Prepara valores: ORDEN toma la referencia externa si existe.
    orden_values = df[ORDEN_SOURCE_COLUMN].astype(str) if ORDEN_SOURCE_COLUMN in df.columns else ""

    # Inserta 3 columnas vacias de separacion.
    for idx in range(GAP_COLUMNS):
        df.insert(insert_pos + idx, f"{GAP_COLUMN_PREFIX}{idx + 1}", "")
    insert_pos = insert_pos + GAP_COLUMNS

    # Inserta en orden manteniendo valores vacios en las demas.
    for idx, name in enumerate(NEW_COLUMNS):
        values = orden_values if name == "ORDEN" else ""
        df.insert(insert_pos + idx, name, values)

    return df


def _render_headers(cols: list[str]) -> list[str]:
    # Convierte columnas de separacion a encabezados vacios.
    rendered = []
    for col in cols:
        if col.startswith(GAP_COLUMN_PREFIX):
            rendered.append("")
        else:
            rendered.append(col)
    return rendered


def _rows_to_text(rows: Iterable[Iterable[Any]]) -> list[tuple[str, ...]]:
    normalized: list[tuple[str, ...]] = []
    for row in rows:
        normalized.append(tuple(_to_text(value) for value in row))
    return normalized


def _to_date(value: Any) -> date | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        parsed = pd.to_datetime(value, errors="coerce")
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    return parsed.date()


def _to_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)
