# Contracargos MP

Genera el reporte de contracargos en Excel a partir del archivo fuente y consultas en PostgreSQL.

## Requisitos
- Python 3.10+ (probado en Windows)
- Dependencias listadas en `requirements.txt`

## Configuración
1. Copia `.env.example` a `.env` y completa tus credenciales.
2. Verifica las rutas en `.env`:
   - `CONTRACARGO_ORIGEN_PATH`
   - `CONTRACARGO_SALIDA_PATH`
   - `VENTAS_RUTA_PATH`
   - `VENTAS_RUTA_PATH2`

## Ejecución
```powershell
python -m Contracargo_reporte.run_contracargo_mp
```

## Notas
- La hoja principal se llama `Data`.
- El reporte se genera en la ruta configurada o la indicada por CLI.
