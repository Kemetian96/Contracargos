from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import re
import pandas as pd

from .infrastructure.config import Settings, load_settings
from .infrastructure.db.repository import PostgresRepository
from .infrastructure.export import exportar_pestana_texto, eliminar_pestanas


LOGGER = logging.getLogger(__name__)


SOURCE_SHEET_NAME = "Export after collection"
DATA_SHEET_NAME = "Data"

NEW_COLUMNS = [
    "ORDEN",
    "RMA",
    "TIPO DE FACTURADA",
    "TIPO DE ENTREGA",
    "TIENDA DESTINO",
    "UBIGEO - DESTINO",
    "DEPARTAMENTO",
    "ESTADO MP",
    "ESTADO FINAL",
    "DNI",
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
    - Columna RMA se llena desde RmaxOrder.sql segun ORDEN.
    """
    settings = settings or load_settings()

    repo = PostgresRepository(settings)
    df = _cargar_y_transformar(paths.origen_excel)
    order_col = _resolve_order_column(df)
    ordenes = _extraer_ordenes(df, order_col)
    LOGGER.info("Columna ORDEN detectada: %s | Ordenes encontradas: %s", order_col, len(ordenes))
    if ordenes:
        LOGGER.info("Ejemplo ORDEN: %s", ordenes[:5])
    if not ordenes:
        LOGGER.warning("No se encontraron valores de ORDEN en la columna '%s'.", ORDEN_SOURCE_COLUMN)
    rma_totales_rows, _rma_totales_cols = repo.obtener_rmas_totales_por_ordenes(ordenes)
    rma_diff_map = _build_rma_diff_map(rma_totales_rows)
    rma_final_map_raw, rma_order_totals = repo.obtener_rmas_finales_por_ordenes(ordenes)
    dni_map_raw = repo.obtener_dni_por_ordenes(ordenes)
    dni_map = {_normalize_order_value(k): _normalize_order_value(v) for k, v in dni_map_raw.items()}
    rma_final_map = {_normalize_order_value(k): v for k, v in rma_final_map_raw.items()}
    rma_diff_map = _build_rma_diff_map_from_chain(rma_final_map, rma_order_totals)
    rma_map = _build_rma_concat_map(rma_final_map)
    LOGGER.info("RMA finales encontrados: %s", len(rma_map))
    if rma_map:
        LOGGER.info("Ejemplo RMA finales: %s", list(rma_map.items())[:5])
    non_empty_rma = sum(1 for v in rma_map.values() if v)
    if non_empty_rma == 0 and rma_map:
        LOGGER.warning("Todos los RMA finales vienen vacios. Revisa la logica de cadena.")
    else:
        LOGGER.info("RMA no vacios: %s", non_empty_rma)
    tipo_entrega_raw, tienda_raw, ubigeo_raw = repo.obtener_tipo_entrega_por_ordenes(ordenes)
    tipo_entrega_map = {_normalize_order_value(k): _map_tipo_entrega(v) for k, v in tipo_entrega_raw.items()}
    tienda_map = {_normalize_order_value(k): v for k, v in tienda_raw.items()}
    ubigeo_map = {_normalize_order_value(k): _normalize_order_value(v) for k, v in ubigeo_raw.items()}
    departamento_map = {_normalize_order_value(k): _map_departamento(v) for k, v in ubigeo_raw.items()}
    tienda_map = _clear_tienda_for_vale(tienda_map, tipo_entrega_map)

    missing_for_fallback = _ordenes_con_campos_vacios(ordenes, tienda_map, ubigeo_map, departamento_map)
    if missing_for_fallback:
        fb_tipo_raw, fb_ubigeo_raw = repo.obtener_tipo_entrega_fallback(missing_for_fallback)
        fb_ubigeo_map = {_normalize_order_value(k): _normalize_order_value(v) for k, v in fb_ubigeo_raw.items()}
        fb_departamento_map = {_normalize_order_value(k): _map_departamento(v) for k, v in fb_ubigeo_raw.items()}
        for order in missing_for_fallback:
            key = _normalize_order_value(order)
            # Solo llena si estaba vacio.
            if not tienda_map.get(key):
                tienda_map[key] = "DOMICILIO"
            if not ubigeo_map.get(key):
                ubigeo_map[key] = fb_ubigeo_map.get(key, "")
            if not departamento_map.get(key):
                departamento_map[key] = fb_departamento_map.get(key, "")
        tienda_map = _clear_tienda_for_vale(tienda_map, tipo_entrega_map)
    # Estados finales para VALE segun egiftcards
    vale_orders = [o for o in ordenes if tipo_entrega_map.get(_normalize_order_value(o)) == "VALE"]
    egift_status_raw = repo.obtener_egift_status_por_ordenes(vale_orders)
    vale_final_map = _build_vale_final_map(egift_status_raw)
    orders_status_raw = repo.obtener_orders_status_por_ordenes(vale_orders)
    vale_status_map = _build_vale_status_map(orders_status_raw)

    if ordenes:
        LOGGER.info("ORDENES enviadas: %s | RMAs encontrados: %s", len(ordenes), len(rma_map))
    facturada_map = _load_facturada_map(paths.salida_excel)
    df = _insertar_columnas_custom(
        df,
        rma_map,
        tipo_entrega_map,
        tienda_map,
        ubigeo_map,
        departamento_map,
        rma_diff_map,
        dni_map,
        facturada_map,
        vale_final_map,
        vale_status_map,
        order_col,
    )

    data_cols = _render_headers(list(df.columns))
    data_rows = _df_to_rows(df, date_columns=DATE_COLUMNS)

    exportar_pestana_texto(
        rows=data_rows,
        cols=data_cols,
        ruta=paths.salida_excel,
        sheet_name=DATA_SHEET_NAME,
        date_columns=DATE_COLUMNS,
        date_format="dd/mm/yyyy",
    )
    eliminar_pestanas(paths.salida_excel, ["RMA_Totales_TMP", "RMA_Final_TMP"])


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

    return df.reset_index(drop=True)


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


def _insertar_columnas_custom(
    df: pd.DataFrame,
    rma_map: dict[str, str],
    tipo_entrega_map: dict[str, str],
    tienda_map: dict[str, str],
    ubigeo_map: dict[str, str],
    departamento_map: dict[str, str],
    rma_diff_map: dict[str, str],
    dni_map: dict[str, str],
    facturada_map: dict[str, str],
    vale_final_map: dict[str, str],
    vale_status_map: dict[str, str],
    order_col: str | None,
) -> pd.DataFrame:
    # Inserta columnas nuevas despues de las columnas existentes + 3 columnas vacias.
    cols = list(df.columns)
    insert_pos = len(cols)

    # Prepara valores: ORDEN toma la referencia externa si existe.
    if order_col and order_col in df.columns:
        orden_values = df[order_col].apply(_normalize_order_value)
    else:
        orden_values = ""

    # Inserta 3 columnas vacias de separacion.
    for idx in range(GAP_COLUMNS):
        df.insert(insert_pos + idx, f"{GAP_COLUMN_PREFIX}{idx + 1}", "")
    insert_pos = insert_pos + GAP_COLUMNS

    # Inserta en orden manteniendo valores vacios en las demas.
    for idx, name in enumerate(NEW_COLUMNS):
        if name == "ORDEN":
            values = orden_values
        elif name == "RMA":
            values = orden_values.map(lambda v: rma_map.get(v, ""))
        elif name == "TIPO DE FACTURADA":
            values = orden_values.map(lambda v: facturada_map.get(v, ""))
        elif name == "TIPO DE ENTREGA":
            values = orden_values.map(lambda v: tipo_entrega_map.get(v, ""))
        elif name == "TIENDA DESTINO":
            values = orden_values.map(lambda v: tienda_map.get(v, ""))
        elif name == "UBIGEO - DESTINO":
            values = orden_values.map(lambda v: ubigeo_map.get(v, ""))
        elif name == "DEPARTAMENTO":
            values = orden_values.map(lambda v: departamento_map.get(v, ""))
        elif name == "ESTADO MP":
            if "Estado" in df.columns:
                base = df["Estado"].map(_map_estado_mp).fillna("")
            else:
                base = ""
            if isinstance(base, pd.Series):
                values = base
            else:
                values = base
        elif name == "ESTADO FINAL":
            values = _map_estado_final(df, rma_diff_map, vale_final_map, vale_status_map, order_col)
        elif name == "DNI":
            values = orden_values.map(lambda v: dni_map.get(v, ""))
        else:
            values = ""
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
    text = str(value)
    return "" if text.strip().lower() == "nan" else text


def _extraer_ordenes(df: pd.DataFrame, order_col: str | None) -> list[str]:
    if not order_col or order_col not in df.columns:
        return []
    series = df[order_col].apply(_normalize_order_value)
    values = [value for value in series.tolist() if value]
    # De-duplica manteniendo orden.
    seen: set[str] = set()
    ordenes: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordenes.append(value)
    return ordenes


def _normalize_order_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if pd.isna(value):
            return ""
        if value.is_integer():
            return str(int(value))
        return str(value).strip()
    if isinstance(value, (int,)):
        return str(value)
    text = str(value).strip().upper()
    if text.lower() in {"", "nan", "none"}:
        return ""
    # Quita sufijo .0 si vino como string numerico.
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        return text[:-2]
    return text


def _map_tipo_entrega(value: Any) -> str:
    text = _normalize_order_value(value)
    if text == "1":
        return "Domicilio"
    if text == "2":
        return "Tienda"
    if text == "3":
        return "Tienda"
    if text == "4":
        return "VALE"
    if text == "5":
        return "Tienda"    
    return ""


def _map_estado_mp(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    mapping = {
        "dispute": "EN DISPUTA",
        "settled": "CERRADO EN CONTRA",
        "covered": "CUBIERTO",
        "documentation_pending": "PENDIENTE DE DOCUMENTACION",
        "reimbursed": "CUBIERTO",
    }
    return mapping.get(text, "")


def _map_estado_final(
    df: pd.DataFrame,
    rma_diff_map: dict[str, str],
    vale_final_map: dict[str, str],
    vale_status_map: dict[str, str],
    order_col: str | None,
) -> pd.Series:
    if "Estado" in df.columns:
        base_mp = df["Estado"].map(_map_estado_mp).fillna("")
    else:
        base_mp = pd.Series([""] * len(df))

    mapping = {
        "CUBIERTO": "NO PERDIDA",
        "PENDIENTE DE DOCUMENTACION": "PENDIENTE",
        "CERRADO EN CONTRA": "PERDIDA",
        "EN DISPUTA":"PENDIENTE",
    }
    estado_final = base_mp.map(lambda v: mapping.get(v, ""))
    if order_col and order_col in df.columns:
        orden_values = df[order_col].apply(_normalize_order_value)
        if rma_diff_map:
            # Override por totales antes del ajuste VALE.
            override = orden_values.map(lambda v: rma_diff_map.get(v, ""))
            estado_final = override.where(override != "", estado_final)
        if vale_final_map:
            # Override VALE al final de toda la logica.
            override_vale = orden_values.map(lambda v: vale_final_map.get(v, ""))
            estado_final = override_vale.where(override_vale != "", estado_final)
        if vale_status_map:
            # Si es VALE y status < 0 => NO PERDIDA (al final).
            override_vale_status = orden_values.map(lambda v: vale_status_map.get(v, ""))
            estado_final = override_vale_status.where(override_vale_status != "", estado_final)
    # Si ESTADO MP es CUBIERTO, fuerza NO PERDIDA por encima de todo.
    cubierto_mask = base_mp == "CUBIERTO"
    if cubierto_mask.any():
        estado_final = estado_final.where(~cubierto_mask, "NO PERDIDA")
    return estado_final


def _build_rma_diff_map_from_chain(
    rma_final_map: dict[str, list[tuple[str, str, str]]],
    order_totals: dict[str, Any],
) -> dict[str, str]:
    diff_map: dict[str, str] = {}
    for order, finals in rma_final_map.items():
        if order not in order_totals or not finals:
            continue
        # Regla: si hay un RMA tipo 4 -> NO PERDIDA
        if any(str(rma_type).strip() == "4" for _uid, rma_type, _total in finals):
            diff_map[order] = "NO PERDIDA"
            continue
        total_order = _parse_decimal_local(order_totals[order])
        suma_rmas = Decimal("0.00")
        tiene_rma_tipo = False
        for _uid_rma, rma_type, rma_total in finals:
            if str(rma_type).strip() not in {"2", "5"}:
                continue
            if rma_total is None or str(rma_total).strip() == "":
                continue
            tiene_rma_tipo = True
            suma_rmas += _parse_decimal_local(rma_total)
        if not tiene_rma_tipo:
            continue
        diff = total_order - suma_rmas
        if diff.quantize(Decimal("0.01")) == Decimal("0.00"):
            diff_map[order] = "NO PERDIDA"
        else:
            diff_map[order] = "PERDIDA"
    return diff_map


def _build_rma_concat_map(
    rma_final_map: dict[str, list[tuple[str, str, str]]],
) -> dict[str, str]:
    """Concatena RMAs finales de tipo 2 y 5 separados por '-'."""
    concat_map: dict[str, str] = {}
    for order, finals in rma_final_map.items():
        if not finals:
            continue
        filtered = [uid for uid, rma_type, _total in finals if str(rma_type).strip() in {"2", "5"} and uid]
        if filtered:
            concat_map[order] = "-".join(filtered)
    return concat_map


def _build_rma_diff_map(rows: list[tuple[Any, ...]]) -> dict[str, str]:
    # Deprecated: kept for compatibility if needed.
    diff_map: dict[str, str] = {}
    for row in rows:
        if not row:
            continue
        uid_order = _normalize_order_value(row[0]) if len(row) > 0 else ""
        total_order = row[1] if len(row) > 1 else None
        total_rma = row[3] if len(row) > 3 else None
        if not uid_order or total_order is None or total_rma is None:
            continue
        diff = _decimal_diff(total_order, total_rma)
        if diff is None:
            continue
        diff_map[uid_order] = "NO PERDIDA" if diff == 0 else "PERDIDA"
    return diff_map


def _build_vale_final_map(status_map: dict[str, str]) -> dict[str, str]:
    vale_map: dict[str, str] = {}
    for order, status in status_map.items():
        key = _normalize_order_value(order)
        if status == "5":
            vale_map[key] = "NO PERDIDA"
        elif status in {"1", "2", "3"}:
            vale_map[key] = "PENDIENTE"
    return vale_map


def _build_vale_status_map(status_map: dict[str, str]) -> dict[str, str]:
    vale_map: dict[str, str] = {}
    for order, status in status_map.items():
        key = _normalize_order_value(order)
        try:
            if int(str(status).strip()) < 0:
                vale_map[key] = "NO PERDIDA"
        except ValueError:
            continue
    return vale_map




def _decimal_diff(a: Any, b: Any) -> int | None:
    try:
        da = _parse_decimal_local(a)
        db = _parse_decimal_local(b)
    except InvalidOperation:
        return None
    return 0 if da.quantize(Decimal("0.01")) == db.quantize(Decimal("0.01")) else 1


def _parse_decimal_local(value: Any) -> Decimal:
    if value is None:
        raise InvalidOperation
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if text == "":
        raise InvalidOperation
    cleaned = []
    for ch in text:
        if ch.isdigit() or ch in {".", ",", "-"}:
            cleaned.append(ch)
    text = "".join(cleaned)
    if "," in text and "." in text:
        text = text.replace(",", "")
    elif "," in text and "." not in text:
        text = text.replace(",", ".")
    return Decimal(text)


def _resolve_order_column(df: pd.DataFrame) -> str | None:
    if ORDEN_SOURCE_COLUMN in df.columns:
        return ORDEN_SOURCE_COLUMN
    # Fallback: busca columna que contenga "referencia externa" ignorando mayusculas/tildes.
    normalized_targets = ["referencia externa", "referencia externa de la transaccion", "referencia externa de la transacción"]
    for col in df.columns:
        col_norm = _normalize_text(col)
        if any(target in col_norm for target in normalized_targets):
            return col
    return None


def _normalize_text(value: Any) -> str:
    text = str(value).strip().lower()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return text


def _load_facturada_map(path: Path) -> dict[str, str]:
    ruta = Path(path)
    if not ruta.exists():
        return {}
    try:
        df_old = pd.read_excel(ruta, sheet_name=DATA_SHEET_NAME)
    except Exception:
        # Si no se puede leer, no preserva valores.
        return {}
    if "ORDEN" not in df_old.columns or "TIPO DE FACTURADA" not in df_old.columns:
        return {}
    facturada_map: dict[str, str] = {}
    for _, row in df_old.iterrows():
        orden = _normalize_order_value(row.get("ORDEN"))
        valor = row.get("TIPO DE FACTURADA")
        if orden and valor is not None and not pd.isna(valor):
            text = str(valor).strip()
            if text and text.lower() != "nan":
                facturada_map[orden] = text
    return facturada_map


def _map_departamento(value: Any) -> str:
    code = _normalize_order_value(value)
    if not code:
        return ""
    if code.isdigit():
        code = code.zfill(6)
    # Usa los 2 primeros digitos del ubigeo.
    code2 = code[:2].zfill(2)
    mapping = {
        "01": "AMAZONAS",
        "02": "ANCASH",
        "03": "APURÍMAC",
        "04": "AREQUIPA",
        "05": "AYACUCHO",
        "06": "CAJAMARCA",
        "07": "CALLAO",
        "08": "CUSCO",
        "09": "HUANCAVELICA",
        "10": "HUÁNUCO",
        "11": "ICA",
        "12": "JUNÍN",
        "13": "LA LIBERTAD",
        "14": "LAMBAYEQUE",
        "15": "LIMA",
        "16": "LORETO",
        "17": "MADRE DE DIOS",
        "18": "MOQUEGUA",
        "19": "PASCO",
        "20": "PIURA",
        "21": "PUNO",
        "22": "SAN MARTÍN",
        "23": "TACNA",
        "24": "TUMBES",
        "25": "UCAYALI",
    }
    return mapping.get(code2, "")


def _ordenes_con_campos_vacios(
    ordenes: list[str],
    tienda_map: dict[str, str],
    ubigeo_map: dict[str, str],
    departamento_map: dict[str, str],
) -> list[str]:
    faltantes: list[str] = []
    for order in ordenes:
        key = _normalize_order_value(order)
        if not key:
            continue
        if not tienda_map.get(key) or not ubigeo_map.get(key) or not departamento_map.get(key):
            faltantes.append(key)
    return faltantes


def _clear_tienda_for_vale(
    tienda_map: dict[str, str],
    tipo_entrega_map: dict[str, str],
) -> dict[str, str]:
    resultado = dict(tienda_map)
    for order, tipo in tipo_entrega_map.items():
        if tipo == "VALE":
            resultado[order] = ""
    return resultado
