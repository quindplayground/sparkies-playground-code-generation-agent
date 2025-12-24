"""Step 300: Aggregate products and compress.

This step groups records by the combination of cod_transaccional + cod_org_vent
+ cod_canal + cod_vendedor, collects all material records into a productos
array, excludes specified columns, and compresses to GZIP JSON.
"""

import gzip
import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.types import BinaryType

from quind_demo_ppd_project.libs.logging import get_logger

logger = get_logger(__name__)


def _compress_json_udf():
    """Create UDF to compress JSON array to GZIP binary."""
    def compress_json(json_str: str) -> bytes | None:
        if json_str is None:
            return None
        try:
            json_bytes = json_str.encode("utf-8")
            compressed = gzip.compress(json_bytes, compresslevel=9)
            return compressed
        except Exception:
            return None

    return sf.udf(compress_json, BinaryType())


def step_300_aggregate_products(dataframe: DataFrame) -> DataFrame:
    """Aggregate products by combination and compress to GZIP JSON.

    Groups by pk, sk, gsi1_pk, gsi1_sk, gsi2_pk, gsi2_sk and collects
    all material records into a productos array. Excludes specified columns
    from the productos array and compresses to GZIP binary.

    Args:
        dataframe: Input DataFrame with DynamoDB keys and portfolio data.

    Returns:
        DataFrame with aggregated productos array compressed as GZIP binary.
    """
    excluded_columns = [
        "cod_condicion_pago_area_venta",
        "cod_tipo_jerarquia",
        "cod_clas_fiscal_cliente_1",
        "cod_clas_fiscal_cliente_2",
        "cod_clas_fiscal_material_1",
        "cod_clas_fiscal_material_2",
        "cod_categoria",
        "cod_subcategoria",
        "cod_linea",
        "cod_sublinea",
        "cod_marca",
        "cod_submarca",
        "ind_material_activo",
        "ind_cliente_activo",
        "cod_secuencia_exclusion",
        "cod_clase_condicion_exclusion",
        "cod_secuencia_sustitucion",
        "cod_clase_condicion_sustitucion",
        "ban_pb_ind_cliente",
        "ban_bloqueo_soporte_ind_cliente",
        "ban_bloqueo_pedido_area_venta_cliente",
        "ban_pb_area_venta_cliente",
        "ban_pb_niv_canal_material",
        "ban_pb_mandante_material",
        "des_aplica_ibua_en_ventas",
        "des_calculo_valor_ibua",
        "des_hash_contenido_registro",
        "pk",
        "sk",
        "gsi1_pk",
        "gsi1_sk",
        "gsi2_pk",
        "gsi2_sk",
        "fec_actualizacion_dl",
    ]

    all_columns = dataframe.columns
    product_columns = [col for col in all_columns if col not in excluded_columns]

    if not product_columns:
        logger.warning("No columns available for productos array after exclusions")

    struct_fields = [sf.col(col).alias(col) for col in product_columns]
    product_struct = sf.struct(*struct_fields)

    compress_udf = _compress_json_udf()

    aggregated = (
        dataframe
        .repartition("pk", "sk", "gsi1_pk", "gsi1_sk", "gsi2_pk", "gsi2_sk")
        .groupBy(
            "pk", "sk", "gsi1_pk", "gsi1_sk", "gsi2_pk", "gsi2_sk"
        )
        .agg(
            sf.collect_list(product_struct).alias("productos_list"),
            sf.max("fec_actualizacion_dl").alias("fec_actualizacion_dl")
        )
        .select(
            "pk",
            "sk",
            "gsi1_pk",
            "gsi1_sk",
            "gsi2_pk",
            "gsi2_sk",
            compress_udf(sf.to_json(sf.col("productos_list"))).alias("productos"),
            "fec_actualizacion_dl"
        )
    )

    return aggregated
