"""Step 400: Format timestamp.

This step extracts fecha_actualizacion from fec_actualizacion_dl and formats
it as ISO 8601 timestamp string.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_400_format_timestamp(dataframe: DataFrame) -> DataFrame:
    """Format fecha_actualizacion as ISO 8601 timestamp.

    Converts fec_actualizacion_dl to ISO 8601 format string.

    Args:
        dataframe: Input DataFrame with fec_actualizacion_dl column.

    Returns:
        DataFrame with fecha_actualizacion column added in ISO 8601 format.
    """
    result = dataframe.withColumn(
        "fecha_actualizacion",
        sf.date_format(
            sf.col("fec_actualizacion_dl").cast("timestamp"),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        )
    ).select(
        "pk",
        "sk",
        "gsi1_pk",
        "gsi1_sk",
        "gsi2_pk",
        "gsi2_sk",
        "productos",
        "fecha_actualizacion"
    )

    return result
