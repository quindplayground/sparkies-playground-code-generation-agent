"""Step 400: Format output for DynamoDB.

This step formats the final output with proper column names and data types
for DynamoDB loading.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_400_format_output(dataframe: DataFrame) -> DataFrame:
    """Format output DataFrame for DynamoDB.

    Renames columns and formats data:
        - cod_transaccional -> pk
        - sk -> sk
        - gsi1_pk -> gsi1_pk
        - gsi1_sk -> gsi1_sk
        - gsi2_pk -> gsi2_pk
        - gsi2_sk -> gsi2_sk
        - productos -> productos (converted to JSON string)
        - fec_actualizacion_dl -> fecha_actualizacion (ISO format)

    Args:
        dataframe: Input DataFrame with columns:
            - cod_transaccional
            - sk
            - gsi1_pk
            - gsi1_sk
            - gsi2_pk
            - gsi2_sk
            - productos: Array of structs
            - fec_actualizacion_dl: Timestamp string

    Returns:
        Formatted DataFrame with columns:
            - pk: Primary key
            - sk: Sort key
            - gsi1_pk: GSI1 partition key
            - gsi1_sk: GSI1 sort key
            - gsi2_pk: GSI2 partition key
            - gsi2_sk: GSI2 sort key
            - productos: JSON string
            - fecha_actualizacion: ISO formatted timestamp
    """
    result = dataframe.select(
        sf.col("cod_transaccional").alias("pk"),
        sf.col("sk"),
        sf.col("gsi1_pk"),
        sf.col("gsi1_sk"),
        sf.col("gsi2_pk"),
        sf.col("gsi2_sk"),
        sf.to_json(sf.col("productos")).alias("productos"),
        sf.date_format(
            sf.to_timestamp("fec_actualizacion_dl", "yyyy-MM-dd HH:mm:ss"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        ).alias("fecha_actualizacion")
    )
    
    return result.coalesce(200)
