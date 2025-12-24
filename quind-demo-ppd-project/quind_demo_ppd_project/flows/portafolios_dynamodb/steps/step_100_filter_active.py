"""Step 100: Filter active records.

This step filters records to keep only those where both ind_cliente_activo
and ind_material_activo are true.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_100_filter_active(dataframe: DataFrame) -> DataFrame:
    """Filter active records.

    Keeps only records where ind_cliente_activo = true AND
    ind_material_activo = true.

    Args:
        dataframe: Input DataFrame with portfolio data.

    Returns:
        DataFrame with only active records.
    """
    filtered = dataframe.filter(
        (sf.col("ind_cliente_activo") == True)
        & (sf.col("ind_material_activo") == True)
    )

    num_partitions = dataframe.rdd.getNumPartitions()
    if num_partitions > 200:
        return filtered.coalesce(200)
    return filtered
