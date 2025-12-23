"""Step 400: Compress productos array to GZIP JSON.

This step converts the productos array to JSON string and compresses it
using GZIP compression.
"""

import gzip
import json
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.types import ArrayType, BinaryType, StringType
from pyspark.sql.udf import UserDefinedFunction


def _compress_json_udf() -> UserDefinedFunction:
    """Create UDF to compress productos array to GZIP JSON.

    Returns:
        UDF function that takes productos array and returns GZIP compressed bytes.
    """

    def compress_productos(productos_array: list[dict[str, Any]] | None) -> bytes | None:
        """Compress productos array to GZIP JSON.

        Args:
            productos_array: List of dictionaries representing productos.

        Returns:
            GZIP compressed JSON bytes, or None if input is None or empty.
        """
        if not productos_array:
            return None

        json_str = json.dumps(productos_array, ensure_ascii=False, default=str)
        compressed = gzip.compress(json_str.encode("utf-8"), compresslevel=9)

        return compressed

    return sf.udf(compress_productos, BinaryType())


def step_400_compress_productos(dataframe: DataFrame) -> DataFrame:
    """Compress productos array to GZIP JSON.

    Converts productos array to JSON string and compresses using GZIP.

    Args:
        dataframe: Input DataFrame with productos array.

    Returns:
        DataFrame with productos compressed as GZIP bytes.
    """
    compress_udf = _compress_json_udf()

    result = dataframe.withColumn(
        "productos",
        compress_udf(sf.col("productos")),
    )

    return result
