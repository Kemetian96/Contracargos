import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from typing import Optional


ROOT_DIR = Path(__file__).resolve().parents[3]
ENV_PATH = ROOT_DIR / ".env"


def _get_env(name: str, default: str | None = None) -> str:
    # Lee variable obligatoria; falla si no existe.
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Falta la variable de entorno obligatoria: {name}")
    return value


def _get_env_alias(primary: str, aliases: list[str], default: str | None = None) -> str:
    # Permite compatibilidad entre nombres nuevos y legacy.
    value = os.getenv(primary)
    if value:
        return value
    for alias in aliases:
        alias_value = os.getenv(alias)
        if alias_value:
            return alias_value
    if default is not None:
        return default
    raise ValueError(f"Falta la variable de entorno obligatoria: {primary}")


def _get_env_optional(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return value


@dataclass(frozen=True)
class Settings:
    # PostgreSQL
    pg_host: str
    pg_name: str
    pg_user: str
    pg_password: str
    pg_port: int
    pg_sslmode: str
    pg_connect_timeout: int
    # Salidas
    pg_output_path: Path
    comparacion_output_path: Path
    # Parametros generales
    reintentos: int
    espera_segundos: int
    ui_width: int
    ui_height: int
    fecha_inicio_default: str
    fecha_fin_default: str
    contracargo_origen_path: Optional[Path]
    contracargo_salida_path: Optional[Path]
    ventas_ruta_path: Optional[Path]
    ventas_ruta_path2: Optional[Path]


def load_settings() -> Settings:
    # Carga .env del proyecto.
    load_dotenv(ENV_PATH)

    # Construye objeto de configuracion tipado.
    return Settings(
        pg_host=_get_env_alias("PG_HOST", ["DB_HOST"]),
        pg_name=_get_env_alias("PG_NAME", ["DB_NAME"], "main"),
        pg_user=_get_env_alias("PG_USER", ["DB_USER"]),
        pg_password=_get_env_alias("PG_PASSWORD", ["DB_PASSWORD"]),
        pg_port=int(_get_env_alias("PG_PORT", ["DB_PORT"], "5432")),
        pg_sslmode=_get_env_alias("PG_SSLMODE", ["DB_SSLMODE"], "require"),
        pg_connect_timeout=int(_get_env_alias("PG_CONNECT_TIMEOUT", ["DB_CONNECT_TIMEOUT"], "10")),
        pg_output_path=Path(_get_env("PG_OUTPUT_PATH", str(Path("OUTPUT") / "TUTATI.xlsx"))),
        comparacion_output_path=Path(_get_env("COMPARACION_OUTPUT_PATH", str(Path("OUTPUT") / "COMPARACION.xlsx"))),
        reintentos=int(_get_env("REINTENTOS_CONEXION", "5")),
        espera_segundos=int(_get_env("ESPERA_REINTENTO_SEGUNDOS", "10")),
        ui_width=int(_get_env("UI_WIDTH", "360")),
        ui_height=int(_get_env("UI_HEIGHT", "260")),
        fecha_inicio_default=_get_env("FECHA_INICIO", "2026-01-01"),
        fecha_fin_default=_get_env("FECHA_FIN", "2026-01-01"),
        contracargo_origen_path=(
            Path(value) if (value := _get_env_optional("CONTRACARGO_ORIGEN_PATH")) else None
        ),
        contracargo_salida_path=(
            Path(value) if (value := _get_env_optional("CONTRACARGO_SALIDA_PATH")) else None
        ),
        ventas_ruta_path=(
            Path(value) if (value := _get_env_optional("VENTAS_RUTA_PATH")) else None
        ),
        ventas_ruta_path2=(
            Path(value) if (value := _get_env_optional("VENTAS_RUTA_PATH2")) else None
        ),
    )
