# BDA-P1

Scripts principales:

- `data_collector.py` descarga los datasets en `landing_zone/<dataset>/<fecha>`.
- `The_Formatted_Zone.py` lee una fecha concreta con `--date` o, si no se indica, usa la ultima disponible para cada dataset.

Ejemplos:

```bash
python data_collector.py --date 2026-04-02
python The_Formatted_Zone.py --date 2026-04-02
```

Notas:

- `The_Formatted_Zone.py` acepta tanto la estructura nueva `landing_zone/<dataset>/<fecha>` como la antigua `landing_zone/<fecha>/<dataset>`.
- `The_Formatted_Zone.py` usa el paquete Python `duckdb`, así que no necesita `duckdb.jar`, Spark ni configuración de Java/Hadoop para ejecutarse.
