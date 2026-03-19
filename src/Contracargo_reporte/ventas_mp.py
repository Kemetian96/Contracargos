from __future__ import annotations

import logging
from pathlib import Path

import csv
import pandas as pd
import re
from collections import defaultdict
import unicodedata

from .infrastructure.export.excel_writer import exportar_pestana_texto


LOGGER = logging.getLogger(__name__)


SUMMARY_SHEET_NAME = "Resumen"
REQUIRED_COLUMNS = {
    "PAYMENT_METHOD_TYPE",
    "TRANSACTION_TYPE",
    "TRANSACTION_AMOUNT",
}
REQUIRED_COLUMNS_PATH2 = {
    "FECHA_DOCUMENTACION",
    "TOTAL",
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
    ruta_dir: Path | None,
    salida_excel: Path,
    ruta_path2: Path | None = None,
    sheet_name: str = SUMMARY_SHEET_NAME,
) -> None:
    totals_mp: dict[str, float] = defaultdict(float)
    totals_tutati: dict[str, float] = defaultdict(float)

    if ruta_dir:
        ruta_dir = Path(ruta_dir)
        if not ruta_dir.exists():
            raise FileNotFoundError(f"No existe la ruta: {ruta_dir}")

        archivos = sorted(
            list(ruta_dir.glob("*.xlsx"))
            + list(ruta_dir.glob("*.xls"))
            + list(ruta_dir.glob("*.csv"))
        )
        if not archivos:
            LOGGER.warning("No se encontraron archivos de ventas en %s", ruta_dir)
        for archivo in archivos:
            try:
                mes, total = _procesar_archivo(archivo)
                totals_mp[mes] += total
            except Exception:  # pragma: no cover - logging defensivo
                LOGGER.exception("Error procesando %s", archivo)

    if ruta_path2:
        ruta_path2 = Path(ruta_path2)
        if not ruta_path2.exists():
            raise FileNotFoundError(f"No existe la ruta: {ruta_path2}")
        try:
            for mes, total in _procesar_path2(ruta_path2).items():
                totals_tutati[mes] += total
        except Exception:  # pragma: no cover - logging defensivo
            LOGGER.exception("Error procesando %s", ruta_path2)

    if not totals_mp and not totals_tutati:
        LOGGER.warning("No se generaron filas de resumen.")
        return

    all_months = set(totals_mp) | set(totals_tutati)
    rows = [
        (
            mes,
            float(totals_mp.get(mes, 0)),
            float(totals_tutati.get(mes, 0)),
        )
        for mes in all_months
    ]
    rows.sort(key=lambda item: _month_sort_key(item[0]))

    cols = ["MES", "TOTAL_MP", "TOTAL_TUTATI"]
    exportar_pestana_texto(
        rows=rows,
        cols=cols,
        ruta=salida_excel,
        sheet_name=sheet_name,
        numeric_columns={"TOTAL_MP", "TOTAL_TUTATI"},
    )


def _procesar_archivo(archivo: Path) -> tuple[str, float]:
    df = _leer_tabla(archivo, required_cols=REQUIRED_COLUMNS)
    norm_map = {_normalize_col_name(col): col for col in df.columns}
    faltantes = {col for col in REQUIRED_COLUMNS if col not in norm_map}
    if faltantes:
        disponibles = ", ".join(sorted(norm_map.keys()))
        raise ValueError(
            f"Columnas faltantes en {archivo.name}: {', '.join(sorted(faltantes))}. "
            f"Disponibles (normalizadas): {disponibles}"
        )

    pm_col = norm_map["PAYMENT_METHOD_TYPE"]
    tr_col = norm_map["TRANSACTION_TYPE"]
    amt_col = norm_map["TRANSACTION_AMOUNT"]

    filtro = df[pm_col].astype(str).str.strip().str.lower().isin(
        {"debit_card", "credit_card"}
    )
    filtro &= df[tr_col].astype(str).str.strip().str.lower().isin(
        {"refund", "settlement"}
    )
    montos = _normalizar_monto_series(df.loc[filtro, amt_col])
    total = float(montos.sum())

    mes = _month_label_from_text(archivo.stem)
    LOGGER.info("%s -> total=%s", archivo.name, total)
    return mes, total


def _procesar_path2(ruta_path2: Path) -> dict[str, float]:
    totales: dict[str, float] = defaultdict(float)
    archivos = [ruta_path2]
    if ruta_path2.is_dir():
        archivos = sorted(
            list(ruta_path2.glob("*.xlsx"))
            + list(ruta_path2.glob("*.xls"))
            + list(ruta_path2.glob("*.csv"))
        )

    for archivo in archivos:
        df = _leer_tabla(archivo, required_cols=REQUIRED_COLUMNS_PATH2)
        norm_map = {_normalize_col_name(col): col for col in df.columns}
        faltantes = {col for col in REQUIRED_COLUMNS_PATH2 if col not in norm_map}
        if faltantes:
            disponibles = ", ".join(sorted(norm_map.keys()))
            raise ValueError(
                f"Columnas faltantes en {archivo.name}: {', '.join(sorted(faltantes))}. "
                f"Disponibles (normalizadas): {disponibles}"
            )

        fecha_col = norm_map["FECHA_DOCUMENTACION"]
        total_col = norm_map["TOTAL"]

        fechas = pd.to_datetime(df[fecha_col], errors="coerce", dayfirst=True)
        montos = _normalizar_monto_series(df[total_col])
        temp = pd.DataFrame({"fecha": fechas, "monto": montos})
        temp = temp[temp["fecha"].notna()]

        for periodo, group in temp.groupby(temp["fecha"].dt.month):
            mes = _month_label_from_number(int(periodo))
            totales[mes] += float(group["monto"].sum())

        LOGGER.info("%s -> meses agrupados=%s", archivo.name, len(temp.groupby(temp["fecha"].dt.month)))

    return totales


def _leer_tabla(archivo: Path, required_cols: set[str] | None = None) -> pd.DataFrame:
    suffix = archivo.suffix.lower()
    if suffix == ".csv":
        try:
            df = _leer_csv_flexible(archivo, delimiter=",")
            if required_cols and not _tiene_columnas(df, required_cols):
                df = _leer_csv_flexible(archivo, delimiter=";")
            if required_cols and not _tiene_columnas(df, required_cols):
                df = _leer_csv_flexible(archivo)
            return df
        except Exception:
            LOGGER.warning("CSV malformado en %s, reintentando con on_bad_lines='skip'.", archivo.name)
            df = pd.read_csv(
                archivo,
                sep=",",
                engine="python",
                encoding="utf-8-sig",
                on_bad_lines="skip",
            )
            if df.shape[1] == 1:
                df = pd.read_csv(
                    archivo,
                    sep=";",
                    engine="python",
                    encoding="utf-8-sig",
                    on_bad_lines="skip",
                )
            return df
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
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", "_", text)
    return text.upper()


def _normalizar_monto_series(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.strip()
    raw = raw.str.replace("\u00a0", "", regex=False)
    raw = raw.str.replace(" ", "", regex=False)
    raw = raw.str.replace(r"[^0-9,.\-]", "", regex=True)

    has_comma = raw.str.contains(",", na=False)
    has_dot = raw.str.contains(r"\.", na=False)
    both = has_comma & has_dot
    only_comma = has_comma & ~has_dot

    raw = raw.mask(both, raw.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
    raw = raw.mask(only_comma, raw.str.replace(",", ".", regex=False))

    return pd.to_numeric(raw, errors="coerce").fillna(0)


def _detectar_delimitador(archivo: Path) -> str:
    with archivo.open("r", encoding="utf-8-sig", errors="ignore") as file:
        for line in file:
            raw = line.strip()
            if raw:
                comma = raw.count(",")
                semicolon = raw.count(";")
                return ";" if semicolon > comma else ","
    return ","


def _leer_csv_flexible(archivo: Path, delimiter: str | None = None) -> pd.DataFrame:
    delimiter = delimiter or _detectar_delimitador(archivo)
    with archivo.open("r", encoding="utf-8-sig", errors="ignore", newline="") as file:
        reader = csv.reader(file, delimiter=delimiter, quotechar='"')
        rows = list(reader)

    if not rows:
        return pd.DataFrame()

    header = rows[0]
    data = []
    header_len = len(header)
    for row in rows[1:]:
        if len(row) < header_len:
            row = row + [""] * (header_len - len(row))
        elif len(row) > header_len:
            row = row[:header_len]
        data.append(row)

    return pd.DataFrame(data, columns=header)


def _tiene_columnas(df: pd.DataFrame, required_cols: set[str]) -> bool:
    if df.empty:
        return False
    norm_map = {_normalize_col_name(col): col for col in df.columns}
    return all(col in norm_map for col in required_cols)


def _month_sort_key(raw_name: str) -> tuple[int, str]:
    normalized = raw_name.strip().lower()
    for month, order in MONTH_ORDER.items():
        if month in normalized:
            return order, normalized
    return 99, normalized


def _month_label_from_text(raw_name: str) -> str:
    normalized = raw_name.strip().lower()
    for month, order in MONTH_ORDER.items():
        if month in normalized:
            return _month_label_from_number(order)
    return raw_name.strip()


def _month_label_from_number(month_number: int) -> str:
    mapping = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre",
    }
    return mapping.get(month_number, str(month_number))
