"""Step 100: Filter active records.

This step filters records to keep only those where both
ind_cliente_activo and ind_material_activo are true.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_100_filter_active_records(dataframe: DataFrame) -> DataFrame:
    """Filter active records.

    Keeps only records where ind_cliente_activo = true AND
    ind_material_activo = true. Filtering is done early to reduce
    data volume for subsequent operations.

    Args:
        dataframe: Input DataFrame with portfolio data.

    Returns:
        DataFrame filtered to active records only.
    """
    filtered = dataframe.filter(
        (sf.col("ind_cliente_activo") == True)
        & (sf.col("ind_material_activo") == True)
    )

    return filtered
