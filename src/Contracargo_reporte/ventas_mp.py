from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from pandas.errors import ParserError
import re

from .infrastructure.export.excel_writer import exportar_pestana_texto


LOGGER = logging.getLogger(__name__)


SUMMARY_SHEET_NAME = "Resumen"
REQUIRED_COLUMNS = {
    "PAYMENT_METHOD_TYPE",
    "TRANSACTION_AMOUNT",
}
MONTH_ORDER = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def generar_resumen_ventas_mp(
    ruta_dir: Path,
    salida_excel: Path,
    sheet_name: str = SUMMARY_SHEET_NAME,
) -> None:
    ruta_dir = Path(ruta_dir)
    if not ruta_dir.exists():
        raise FileNotFoundError(f"No existe la ruta: {ruta_dir}")

    archivos = sorted(
        list(ruta_dir.glob("*.xlsx"))
        + list(ruta_dir.glob("*.xls"))
        + list(ruta_dir.glob("*.csv"))
    )
    if not archivos:
        LOGGER.warning("No se encontraron .xlsx en %s", ruta_dir)
        return

    rows = []
    for archivo in archivos:
        try:
            rows.append(_procesar_archivo(archivo))
        except Exception:  # pragma: no cover - logging defensivo
            LOGGER.exception("Error procesando %s", archivo)

    if not rows:
        LOGGER.warning("No se generaron filas de resumen.")
        return

    rows.sort(key=lambda item: _month_sort_key(item[0]))

    cols = ["MES", "TOTAL_MES"]
    exportar_pestana_texto(rows=rows, cols=cols, ruta=salida_excel, sheet_name=sheet_name)


def _procesar_archivo(archivo: Path) -> tuple[str, float]:
    df = _leer_tabla(archivo)
    norm_map = {_normalize_col_name(col): col for col in df.columns}
    faltantes = {col for col in REQUIRED_COLUMNS if col not in norm_map}
    if faltantes:
        disponibles = ", ".join(sorted(norm_map.keys()))
        raise ValueError(
            f"Columnas faltantes en {archivo.name}: {', '.join(sorted(faltantes))}. "
            f"Disponibles (normalizadas): {disponibles}"
        )

    pm_col = norm_map["PAYMENT_METHOD_TYPE"]
    amt_col = norm_map["TRANSACTION_AMOUNT"]

    filtro = df[pm_col].astype(str).str.strip().str.lower().isin(
        {"debit_card", "credit_card"}
    )
    montos = pd.to_numeric(df.loc[filtro, amt_col], errors="coerce")
    total = float(montos.fillna(0).sum())

    LOGGER.info("%s -> total=%s", archivo.name, total)
    return archivo.stem, total


def _leer_tabla(archivo: Path) -> pd.DataFrame:
    suffix = archivo.suffix.lower()
    if suffix == ".csv":
        try:
            return pd.read_csv(archivo, sep=None, engine="python", encoding="utf-8-sig")
        except ParserError:
            LOGGER.warning("CSV malformado en %s, reintentando con on_bad_lines='skip'.", archivo.name)
            return pd.read_csv(
                archivo,
                sep=None,
                engine="python",
                encoding="utf-8-sig",
                on_bad_lines="skip",
            )
    if suffix in {".xlsx", ".xls"}:
        try:
            return pd.read_excel(archivo, engine="openpyxl")
        except Exception:
            LOGGER.warning("Fallo openpyxl en %s, reintentando con calamine.", archivo.name)
            return pd.read_excel(archivo, engine="calamine")
    raise ValueError(f"Tipo de archivo no soportado: {archivo.name}")


def _normalize_col_name(name: object) -> str:
    text = str(name)
    text = text.replace("\ufeff", "")
    text = text.replace("\u00a0", " ")
    text = text.strip()
    text = re.sub(r"\s+", "_", text)
    return text.upper()


def _month_sort_key(raw_name: str) -> tuple[int, str]:
    normalized = raw_name.strip().lower()
    for month, order in MONTH_ORDER.items():
        if month in normalized:
            return order, normalized
    return 99, normalized
