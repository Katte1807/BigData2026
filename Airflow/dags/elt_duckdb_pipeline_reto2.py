from datetime import datetime, timedelta
from airflow.decorators import dag, task
import duckdb
import os

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="elt_duckdb_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="Pipeline ELT incremental con DuckDB orquestado por Airflow",
)
def elt_pipeline():

    @task()
    def preparar_tablas():
        conn = duckdb.connect("dw.duckdb")

        # staging_raw sí puede recrearse porque es temporal
        conn.execute("DROP TABLE IF EXISTS staging_raw")

        # NO borrar la tabla histórica
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_finanzas_elt (
                id INTEGER,
                salario DOUBLE,
                gastos DOUBLE,
                fecha DATE,
                correo_hash STRING,
                utilidad DOUBLE,
                fecha_carga TIMESTAMP,
                archivo_origen STRING
            )
        """)

        # Tabla de control de archivos procesados
        conn.execute("""
            CREATE TABLE IF NOT EXISTS control_archivos (
                archivo STRING PRIMARY KEY,
                fecha_procesado TIMESTAMP,
                registros_procesados INTEGER,
                registros_filtrados INTEGER
            )
        """)

        conn.close()
        return "Tablas preparadas"

    @task()
    def detectar_archivos_nuevos(_mensaje):
        conn = duckdb.connect("dw.duckdb")

        os.makedirs("staging", exist_ok=True)
        archivos_staging = sorted(
            [f for f in os.listdir("staging") if f.endswith(".csv")]
        )

        archivos_ya_procesados = conn.execute("""
            SELECT archivo
            FROM control_archivos
        """).fetchall()

        archivos_ya_procesados = {fila[0] for fila in archivos_ya_procesados}

        archivos_nuevos = [
            f for f in archivos_staging if f not in archivos_ya_procesados
        ]

        print(f"Archivos nuevos detectados: {archivos_nuevos}")

        conn.close()
        return archivos_nuevos

    @task()
    def cargar_staging(archivos_nuevos):
        conn = duckdb.connect("dw.duckdb")

        if not archivos_nuevos:
            conn.close()
            return {
                "archivos": [],
                "total_leidos": 0
            }

        rutas = [f"staging/{archivo}" for archivo in archivos_nuevos]
        rutas_sql = ", ".join([f"'{ruta}'" for ruta in rutas])

        conn.execute(f"""
            CREATE OR REPLACE TABLE staging_raw AS
            SELECT
                *,
                filename AS archivo_origen
            FROM read_csv_auto([{rutas_sql}], filename=True)
        """)

        total_leidos = conn.execute("""
            SELECT COUNT(*)
            FROM staging_raw
        """).fetchone()[0]

        print(f"Total registros leídos en staging_raw: {total_leidos}")

        conn.close()

        return {
            "archivos": archivos_nuevos,
            "total_leidos": total_leidos
        }

    @task()
    def transformar_datos(info_staging):
        conn = duckdb.connect("dw.duckdb")

        archivos = info_staging["archivos"]

        if not archivos:
            conn.close()
            return {
                "archivos_procesados": 0,
                "registros_procesados": 0,
                "registros_filtrados": 0,
                "detalle_archivos": []
            }

        total_leidos = info_staging["total_leidos"]

        conn.execute("DROP TABLE IF EXISTS staging_limpio")

        conn.execute("""
            CREATE TEMP TABLE staging_limpio AS
            SELECT DISTINCT
                id,
                salario,
                COALESCE(gastos, 0) AS gastos,
                CASE
                    WHEN regexp_matches(fecha, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
                        THEN CAST(fecha AS DATE)
                    WHEN regexp_matches(fecha, '^[0-9]{2}/[0-9]{2}/[0-9]{4}$')
                        THEN STRPTIME(fecha, '%d/%m/%Y')
                    WHEN regexp_matches(fecha, '^[0-9]{2}-[0-9]{2}-[0-9]{4}$')
                        THEN STRPTIME(fecha, '%m-%d-%Y')
                    ELSE NULL
                END AS fecha,
                CASE
                    WHEN correo IS NOT NULL THEN sha256(correo)
                    ELSE NULL
                END AS correo_hash,
                salario - COALESCE(gastos, 0) AS utilidad,
                CURRENT_TIMESTAMP AS fecha_carga,
                archivo_origen
            FROM staging_raw
            WHERE
                id IS NOT NULL
                AND fecha IS NOT NULL
                AND salario > 0
                AND COALESCE(gastos, 0) >= 0
        """)

        registros_validos = conn.execute("""
            SELECT COUNT(*)
            FROM staging_limpio
        """).fetchone()[0]

        registros_filtrados = total_leidos - registros_validos

        # Evitar duplicados si se reprocesa parcialmente el mismo archivo
        for archivo in archivos:
            conn.execute(f"""
                DELETE FROM fact_finanzas_elt
                WHERE archivo_origen LIKE '%{archivo}'
            """)

        conn.execute("""
            INSERT INTO fact_finanzas_elt
            SELECT
                id,
                salario,
                gastos,
                fecha,
                correo_hash,
                utilidad,
                fecha_carga,
                archivo_origen
            FROM staging_limpio
        """)

        detalle_archivos = []

        for archivo in archivos:

    procesados_archivo = conn.execute(f"""
        SELECT COUNT(*)
        FROM staging_limpio
        WHERE archivo_origen LIKE '%{archivo}'
    """).fetchone()[0]

    leidos_archivo = conn.execute(f"""
        SELECT COUNT(*)
        FROM staging_raw
        WHERE archivo_origen LIKE '%{archivo}'
    """).fetchone()[0]

    filtrados_archivo = leidos_archivo - procesados_archivo

    conn.execute(f"""
        DELETE FROM control_archivos
        WHERE archivo = '{archivo}'
    """)

    conn.execute(f"""
        INSERT INTO control_archivos (
            archivo,
            fecha_procesado,
            registros_procesados,
            registros_filtrados
        )
        VALUES (
            '{archivo}',
            CURRENT_TIMESTAMP,
            {procesados_archivo},
            {filtrados_archivo}
        )
    """)

    detalle_archivos.append({
        "archivo": archivo,
        "procesados": procesados_archivo,
        "filtrados": filtrados_archivo
    })

            detalle_archivos.append({
                "archivo": archivo,
                "procesados": procesados_archivo,
                "filtrados": filtrados_archivo
            })

        conn.close()

        return {
            "archivos_procesados": len(archivos),
            "registros_procesados": registros_validos,
            "registros_filtrados": registros_filtrados,
            "detalle_archivos": detalle_archivos
        }

    @task()
    def resumen_ejecucion(resumen):
        print("===== RESUMEN DEL DAG =====")
        print(f"Archivos procesados: {resumen['archivos_procesados']}")
        print(f"Registros procesados: {resumen['registros_procesados']}")
        print(f"Registros filtrados: {resumen['registros_filtrados']}")
        print("Detalle por archivo:")

        for item in resumen["detalle_archivos"]:
            print(
                f"- {item['archivo']} | procesados={item['procesados']} | filtrados={item['filtrados']}"
            )

        return "Resumen generado"

    base = preparar_tablas()
    nuevos = detectar_archivos_nuevos(base)
    staging = cargar_staging(nuevos)
    resumen = transformar_datos(staging)
    resumen_ejecucion(resumen)


elt_dag = elt_pipeline()
