from datetime import datetime, timedelta


def fecha_a_cuid(fecha: datetime) -> int:
    # Ajusta al horario del servidor y serializa a entero CUID.
    fecha_servidor = fecha + timedelta(hours=5)
    return int(fecha_servidor.strftime("%Y%m%d%H%M%S") + "00000")


def cuid_a_fecha(cuid: int | str) -> datetime:
    # Convierte CUID (YYYYMMDDHHMMSSxxxxx) a fecha local revirtiendo el offset +5h.
    raw = str(cuid).strip()
    if len(raw) < 14:
        raise ValueError(f"CUID invalido: {cuid}")
    fecha_servidor = datetime.strptime(raw[:14], "%Y%m%d%H%M%S")
    return fecha_servidor - timedelta(hours=5)
