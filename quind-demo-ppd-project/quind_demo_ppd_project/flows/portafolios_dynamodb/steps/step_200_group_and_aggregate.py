"""Step 200: Group and aggregate data.

This step groups records by client, organization, channel, and seller,
and aggregates materials into a products array.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_200_group_and_aggregate(dataframe: DataFrame) -> DataFrame:
    """Group records and aggregate materials into products array.

    Groups records by:
        - cod_transaccional (client code)
        - cod_org_vent (sales organization)
        - cod_canal (channel)
        - cod_vendedor (seller)

    Aggregates:
        - cod_material into a products array
        - fec_actualizacion_dl (takes max value)

    Optimizations applied:
        - Filter early (done in step_100) to reduce data size before aggregation
        - Use column operations (collect_list, max) instead of row operations
        - GroupBy with multiple keys combined in single operation to minimize shuffles

    Args:
        dataframe: Input DataFrame with columns:
            - cod_transaccional: Client transactional code
            - cod_material: Material code
            - cod_org_vent: Sales organization code
            - cod_canal: Channel code
            - cod_vendedor: Seller code
            - fec_actualizacion_dl: Update timestamp

    Returns:
        Aggregated DataFrame with columns:
            - cod_transaccional
            - cod_org_vent
            - cod_canal
            - cod_vendedor
            - productos: Array of material codes
            - fec_actualizacion_dl: Maximum update timestamp
    """
    # GroupBy with all keys in single operation minimizes shuffle overhead
    # Spark optimizes the shuffle automatically based on data distribution
    result = dataframe.groupBy(
        "cod_transaccional",
        "cod_org_vent",
        "cod_canal",
        "cod_vendedor"
    ).agg(
        sf.collect_list("cod_material").alias("productos"),
        sf.max("fec_actualizacion_dl").alias("fec_actualizacion_dl")
    )
    
    return result
