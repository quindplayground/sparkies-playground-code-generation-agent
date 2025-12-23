"""Step 100: Filter active records.

This step filters records to keep only those with active client and active material.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_100_filter_active(dataframe: DataFrame) -> DataFrame:
    """Filter records with active client and active material.

    Args:
        dataframe: Input DataFrame with columns:
            - ind_cliente_activo: Boolean indicating if client is active
            - ind_material_activo: Boolean indicating if material is active

    Returns:
        Filtered DataFrame containing only records where both
        ind_cliente_activo = true AND ind_material_activo = true.
    """
    return dataframe.filter(
        sf.col("ind_cliente_activo") & sf.col("ind_material_activo")
    )
