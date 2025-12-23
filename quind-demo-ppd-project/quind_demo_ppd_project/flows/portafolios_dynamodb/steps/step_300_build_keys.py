"""Step 300: Build DynamoDB keys.

This step constructs the primary key (pk, sk) and global secondary index
keys (gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk) for DynamoDB table structure.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_300_build_keys(dataframe: DataFrame) -> DataFrame:
    """Build DynamoDB primary and secondary index keys.

    Constructs:
        - pk: cod_transaccional (client code)
        - sk: cod_org_vent#cod_canal#cod_vendedor (composite sort key)
        - gsi1_pk: cod_org_vent (organization partition key)
        - gsi1_sk: cod_canal#cod_transaccional (channel#client sort key)
        - gsi2_pk: cod_vendedor (seller partition key)
        - gsi2_sk: cod_transaccional (client sort key)

    Args:
        dataframe: Input DataFrame with columns:
            - cod_transaccional: Client transactional code
            - cod_org_vent: Sales organization code
            - cod_canal: Channel code
            - cod_vendedor: Seller code
            - productos: Array of products
            - fec_actualizacion_dl: Update timestamp

    Returns:
        DataFrame with DynamoDB key columns added:
            - cod_transaccional
            - sk: Composite sort key
            - gsi1_pk: GSI1 partition key
            - gsi1_sk: GSI1 sort key
            - gsi2_pk: GSI2 partition key
            - gsi2_sk: GSI2 sort key
            - productos
            - fec_actualizacion_dl
    """
    return dataframe.select(
        sf.col("cod_transaccional"),
        sf.col("cod_org_vent"),
        sf.col("cod_canal"),
        sf.col("cod_vendedor"),
        sf.col("productos"),
        sf.col("fec_actualizacion_dl"),
        sf.concat_ws("#", "cod_org_vent", "cod_canal", "cod_vendedor").alias("sk"),
        sf.col("cod_org_vent").alias("gsi1_pk"),
        sf.concat_ws("#", "cod_canal", "cod_transaccional").alias("gsi1_sk"),
        sf.col("cod_vendedor").alias("gsi2_pk"),
        sf.col("cod_transaccional").alias("gsi2_sk")
    )
