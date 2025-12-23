"""Step 200: Group and aggregate productos.

This step groups records by cod_transaccional + cod_org_vent + cod_canal + cod_vendedor
and aggregates all material records into a productos array, excluding
specified columns.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def step_200_group_and_aggregate(dataframe: DataFrame) -> DataFrame:
    """Group records and aggregate productos array.

    Groups by cod_transaccional + cod_org_vent + cod_canal + cod_vendedor
    and collects all material records into a productos array, excluding
    columns specified in requirements.

    Args:
        dataframe: Input DataFrame with filtered active records.

    Returns:
        DataFrame grouped by combination with productos array.
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
    ]

    group_by_columns = [
        "cod_transaccional",
        "cod_org_vent",
        "cod_canal",
        "cod_vendedor",
        "fec_actualizacion_dl",
    ]

    excluded_set = set(excluded_columns)
    group_by_set = set(group_by_columns)
    all_columns = dataframe.columns
    productos_columns = [
        col
        for col in all_columns
        if col not in excluded_set and col not in group_by_set
    ]

    productos_struct = sf.struct(*[sf.col(col).alias(col) for col in productos_columns])

    num_partitions = dataframe.rdd.getNumPartitions()
    if num_partitions < 10:
        dataframe = dataframe.repartition(200, *group_by_columns)

    grouped = (
        dataframe.groupBy(*group_by_columns)
        .agg(sf.collect_list(productos_struct).alias("productos"))
    )

    return grouped
