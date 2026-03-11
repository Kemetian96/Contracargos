from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

# Permite ejecutar como script directo y como modulo.
try:
    from .contracargo_mp import ReporteMPPaths, generar_reporte_mp
    from .infrastructure.config import load_settings
except ImportError:  # pragma: no cover - fallback para ejecucion directa
    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from Contracargo_reporte.contracargo_mp import ReporteMPPaths, generar_reporte_mp
    from Contracargo_reporte.infrastructure.config import load_settings


DEFAULT_ORIGEN = Path(
    r"G:\Unidades compartidas\SAC - ADMIN\05.- Reportes\Contracargos_MP\2025\Reporte_Contracargos\Reporte_contracargo_MP_2025.xlsx"
)
DEFAULT_SALIDA = Path(
    r"G:\Unidades compartidas\SAC - ADMIN\05.- Reportes\Contracargos_MP\2025\Reporte_Mp.xlsx"
)


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
    parser.add_argument("--origen", type=Path, default=DEFAULT_ORIGEN, help="Ruta del Excel fuente.")
    parser.add_argument("--salida", type=Path, default=DEFAULT_SALIDA, help="Ruta del Excel destino.")
    parser.add_argument(
        "--fecha-inicio",
        type=_parse_date,
        default=_parse_date(settings.fecha_inicio_default),
        help="Fecha inicio (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--fecha-fin",
        type=_parse_date,
        default=_parse_date(settings.fecha_fin_default),
        help="Fecha fin (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    paths = ReporteMPPaths(origen_excel=args.origen, salida_excel=args.salida)
    generar_reporte_mp(paths=paths, fecha_inicio=args.fecha_inicio, fecha_fin=args.fecha_fin, settings=settings)


if __name__ == "__main__":
    main()
