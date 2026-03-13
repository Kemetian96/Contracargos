from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .infrastructure.export.excel_writer import exportar_pestana_texto


LOGGER = logging.getLogger(__name__)


SUMMARY_SHEET_NAME = "Resumen"
REQUIRED_COLUMNS = {
    "PAYMENT_METHOD_TYPE",
    "TRANSACTION_AMOUNT",
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

    cols = ["Mes", "Suma TRANSACTION_AMOUNT"]
    exportar_pestana_texto(rows=rows, cols=cols, ruta=salida_excel, sheet_name=sheet_name)


def _procesar_archivo(archivo: Path) -> tuple[str, float]:
    df = _leer_tabla(archivo)
    faltantes = REQUIRED_COLUMNS - set(df.columns)
    if faltantes:
        raise ValueError(f"Columnas faltantes en {archivo.name}: {', '.join(sorted(faltantes))}")

    filtro = df["PAYMENT_METHOD_TYPE"].astype(str).str.strip().str.lower().isin(
        {"debit_card", "credit_card"}
    )
    montos = pd.to_numeric(df.loc[filtro, "TRANSACTION_AMOUNT"], errors="coerce")
    total = float(montos.fillna(0).sum())

    LOGGER.info("%s -> total=%s", archivo.name, total)
    return archivo.stem, total


def _leer_tabla(archivo: Path) -> pd.DataFrame:
    suffix = archivo.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(archivo)
    if suffix in {".xlsx", ".xls"}:
        try:
            return pd.read_excel(archivo, engine="openpyxl")
        except Exception:
            LOGGER.warning("Fallo openpyxl en %s, reintentando con calamine.", archivo.name)
            return pd.read_excel(archivo, engine="calamine")
    raise ValueError(f"Tipo de archivo no soportado: {archivo.name}")
