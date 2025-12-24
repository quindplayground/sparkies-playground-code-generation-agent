"""Step 100: Filter active records.

This step filters records to keep only those where both ind_cliente_activo
and ind_material_activo are true.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_100_filter_active(dataframe: DataFrame) -> DataFrame:
    """Filter records to keep only active client and material combinations.

    Args:
        dataframe: Input DataFrame with portfolio data.

    Returns:
        DataFrame containing only records where ind_cliente_activo = true
        AND ind_material_activo = true.
    """
    return dataframe.filter(
        (sf.col("ind_cliente_activo") == True) & (sf.col("ind_material_activo") == True)
    )
