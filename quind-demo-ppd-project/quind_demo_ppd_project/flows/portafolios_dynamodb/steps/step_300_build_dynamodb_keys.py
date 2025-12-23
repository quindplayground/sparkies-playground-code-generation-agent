"""Step 300: Build DynamoDB keys.

This step constructs all DynamoDB keys (PK, SK, GSI1_PK, GSI1_SK, GSI2_PK, GSI2_SK)
from the grouped data and extracts fecha_actualizacion as ISO 8601 timestamp.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_300_build_dynamodb_keys(dataframe: DataFrame) -> DataFrame:
    """Build DynamoDB keys and extract fecha_actualizacion.

    Constructs:
    - pk = cod_transaccional (String)
    - sk = cod_org_vent#cod_canal#cod_vendedor (String)
    - gsi1_pk = cod_org_vent (String)
    - gsi1_sk = cod_canal#cod_transaccional (String)
    - gsi2_pk = cod_vendedor (String)
    - gsi2_sk = cod_transaccional (String)
    - fecha_actualizacion = ISO 8601 timestamp from fec_actualizacion_dl

    Args:
        dataframe: Input DataFrame with grouped data.

    Returns:
        DataFrame with DynamoDB keys and fecha_actualizacion.
    """
    cod_transaccional_str = sf.col("cod_transaccional").cast("string")
    cod_org_vent_str = sf.col("cod_org_vent").cast("string")
    cod_canal_str = sf.col("cod_canal").cast("string")
    cod_vendedor_str = sf.col("cod_vendedor").cast("string")

    fecha_actualizacion_expr = sf.date_format(
        sf.to_utc_timestamp(
            sf.col("fec_actualizacion_dl"),
            sf.lit("America/Bogota"),
        ),
        "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
    )

    result = dataframe.select(
        *dataframe.columns,
        cod_transaccional_str.alias("pk"),
        sf.concat_ws(
            "#",
            cod_org_vent_str,
            cod_canal_str,
            cod_vendedor_str,
        ).alias("sk"),
        cod_org_vent_str.alias("gsi1_pk"),
        sf.concat_ws(
            "#",
            cod_canal_str,
            cod_transaccional_str,
        ).alias("gsi1_sk"),
        cod_vendedor_str.alias("gsi2_pk"),
        cod_transaccional_str.alias("gsi2_sk"),
        fecha_actualizacion_expr.alias("fecha_actualizacion"),
    )

    return result
