"""Step 200: Build DynamoDB keys.

This step constructs all DynamoDB key fields (pk, sk, gsi1_pk, gsi1_sk,
gsi2_pk, gsi2_sk) from the source columns.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_200_build_keys(dataframe: DataFrame) -> DataFrame:
    """Build DynamoDB keys from source columns.

    Constructs:
    - pk = cod_transaccional (String)
    - sk = cod_org_vent#cod_canal#cod_vendedor (String)
    - gsi1_pk = cod_org_vent (String)
    - gsi1_sk = cod_canal#cod_transaccional (String)
    - gsi2_pk = cod_vendedor (String)
    - gsi2_sk = cod_transaccional (String)

    Args:
        dataframe: Input DataFrame with portfolio data.

    Returns:
        DataFrame with DynamoDB key columns added.
    """
    result = dataframe.withColumn(
        "pk",
        sf.col("cod_transaccional").cast("string")
    ).withColumn(
        "sk",
        sf.concat_ws(
            "#",
            sf.col("cod_org_vent").cast("string"),
            sf.col("cod_canal").cast("string"),
            sf.col("cod_vendedor").cast("string")
        )
    ).withColumn(
        "gsi1_pk",
        sf.col("cod_org_vent").cast("string")
    ).withColumn(
        "gsi1_sk",
        sf.concat_ws(
            "#",
            sf.col("cod_canal").cast("string"),
            sf.col("cod_transaccional").cast("string")
        )
    ).withColumn(
        "gsi2_pk",
        sf.col("cod_vendedor").cast("string")
    ).withColumn(
        "gsi2_sk",
        sf.col("cod_transaccional").cast("string")
    )

    return result
