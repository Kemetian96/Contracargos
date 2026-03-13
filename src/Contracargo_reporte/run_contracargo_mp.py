from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

# Permite ejecutar como script directo y como modulo.
try:
    from .contracargo_mp import ReporteMPPaths, generar_reporte_mp
    from .ventas_mp import generar_resumen_ventas_mp
    from .infrastructure.config import load_settings
except ImportError:  # pragma: no cover - fallback para ejecucion directa
    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from Contracargo_reporte.contracargo_mp import ReporteMPPaths, generar_reporte_mp
    from Contracargo_reporte.ventas_mp import generar_resumen_ventas_mp
    from Contracargo_reporte.infrastructure.config import load_settings



def _parse_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        try:
            return datetime.fromisoformat(raw).date()
        except ValueError as inner_exc:
            raise argparse.ArgumentTypeError(
                f"Fecha invalida '{raw}'. Usa formato YYYY-MM-DD."
            ) from inner_exc


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    settings = load_settings()

    parser = argparse.ArgumentParser(description="Genera Reporte MP (Data + Ejemplo).")
    if not settings.contracargo_origen_path or not settings.contracargo_salida_path:
        logging.warning(
            "Rutas de contracargo no configuradas en .env (CONTRACARGO_ORIGEN_PATH / CONTRACARGO_SALIDA_PATH). No se ejecuta."
        )
        return

    parser.add_argument(
        "--origen",
        type=Path,
        default=settings.contracargo_origen_path,
        help="Ruta del Excel fuente.",
    )
    parser.add_argument(
        "--salida",
        type=Path,
        default=settings.contracargo_salida_path,
        help="Ruta del Excel destino.",
    )
    args = parser.parse_args()

    paths = ReporteMPPaths(origen_excel=args.origen, salida_excel=args.salida)
    generar_reporte_mp(
        paths=paths,
        fecha_inicio=_parse_date(settings.fecha_inicio_default),
        fecha_fin=_parse_date(settings.fecha_fin_default),
        settings=settings,
    )
    if settings.ventas_ruta_path:
        generar_resumen_ventas_mp(
            settings.ventas_ruta_path,
            paths.salida_excel,
        )
    else:
        logging.info("VENTAS_RUTA_PATH no configurada, se omite resumen de ventas.")


if __name__ == "__main__":
    main()
