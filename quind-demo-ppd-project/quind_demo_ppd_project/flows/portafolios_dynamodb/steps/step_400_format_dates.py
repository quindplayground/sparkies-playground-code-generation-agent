"""Step 400: Format dates.

This step extracts fecha_actualizacion from fec_actualizacion_dl and formats
it as ISO 8601 timestamp.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_400_format_dates(dataframe: DataFrame) -> DataFrame:
    """Extract and format fecha_actualizacion as ISO 8601 timestamp.

    Converts fec_actualizacion_dl to ISO 8601 format (yyyy-MM-dd'T'HH:mm:ss.SSS'Z').

    Args:
        dataframe: Input DataFrame with aggregated productos and fec_actualizacion_dl.

    Returns:
        DataFrame with fecha_actualizacion column added in ISO 8601 format.
    """
    return dataframe.withColumn(
        "fecha_actualizacion",
        sf.date_format(
            sf.to_timestamp(sf.col("fec_actualizacion_dl")),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        )
    )
