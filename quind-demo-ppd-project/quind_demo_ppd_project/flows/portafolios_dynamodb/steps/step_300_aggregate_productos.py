"""Step 300: Aggregate productos array.

This step groups records by the combination of cod_transaccional, cod_org_vent,
cod_canal, and cod_vendedor, and creates a productos array containing all
material records for each combination, excluding specified columns.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_300_aggregate_productos(dataframe: DataFrame) -> DataFrame:
    """Aggregate productos array by client combination.

    Groups records by pk (cod_transaccional) and sk components (cod_org_vent,
    cod_canal, cod_vendedor), then collects all material records into a productos
    array, excluding columns specified in requirements.

    Args:
        dataframe: Input DataFrame with DynamoDB keys already built.

    Returns:
        DataFrame grouped by pk and sk, with productos array containing
        material records (excluding specified columns).
    """
    excluded_columns = {
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
    }

    all_columns = set(dataframe.columns)
    productos_columns = sorted(list(all_columns - excluded_columns))

    productos_struct = sf.struct([sf.col(col).alias(col) for col in productos_columns])

    aggregated = dataframe.groupBy("pk", "sk", "gsi1_pk", "gsi1_sk", "gsi2_pk", "gsi2_sk").agg(
        sf.collect_list(productos_struct).alias("productos"),
        sf.max("fec_actualizacion_dl").alias("fec_actualizacion_dl")
    )

    return aggregated
