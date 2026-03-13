from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

try:
    from .ventas_mp import generar_resumen_ventas_mp
    from .infrastructure.config import load_settings
except ImportError:  # pragma: no cover - fallback para ejecucion directa
    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from Contracargo_reporte.ventas_mp import generar_resumen_ventas_mp
    from Contracargo_reporte.infrastructure.config import load_settings


DEFAULT_SHEET = "Resumen"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    settings = load_settings()

    if not settings.ventas_ruta_path:
        logging.warning(
            "VENTAS_RUTA_PATH no configurada en .env. No se ejecuta."
        )
        return
    if not settings.contracargo_salida_path:
        logging.warning(
            "CONTRACARGO_SALIDA_PATH no configurada en .env. No se ejecuta."
        )
        return

    parser = argparse.ArgumentParser(description="Resume ventas MP por archivo.")
    parser.add_argument(
        "--ruta",
        type=Path,
        default=settings.ventas_ruta_path,
        help="Carpeta con excels.",
    )
    parser.add_argument(
        "--salida",
        type=Path,
        default=settings.contracargo_salida_path,
        help="Excel destino (se escribe la pestaña Resumen).",
    )
    parser.add_argument("--sheet", default=DEFAULT_SHEET, help="Nombre de la pestaña resumen.")
    args = parser.parse_args()

    generar_resumen_ventas_mp(args.ruta, args.salida, sheet_name=args.sheet)


if __name__ == "__main__":
    main()
