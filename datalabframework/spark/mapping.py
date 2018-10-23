from datalabframework.spark.utils import remove_tones_udf
from pyspark.sql import types

def transform(df, settings):
    """
    :param df:
    :param settings:
    :return:

    read:
        settings:
            extra:
                drop: true
            ma_vt:
                name: sku
                type: string
            ten_vt:
                name: name
                remove_tones: true
                fillna: "unknown"
            sl_ban_ra:
                name: quantity
                type: integer
                fillna: 0
            tien_ban_ra:
                name: revenue
                type: integer
                fillna: -1
            last_date:
                name: date
#                     type: timestamp
                type: date
            year:
#                     value: "quantity/2"
                value: "year(date)"
            month:
                value: "month(date)"
            day:
                value: "day(date)"
            weekday:
                value: "date_format(date, 'EEEE')"
    """

    # drop columns
    drops = []
    for key, value in settings.items():
        if value.get("drop", False):
            drops.append(key)

    # remove dropped fields from settings list
    for key in drops:
        settings.pop(key)

    df = df.drop(*drops)

    mapping = {}
    # do the renaming first
    for key, value in settings.items():
        currentname = key
        rename = value.get("name", None)
        if not (currentname in df.columns) or rename is None:  # new column or no rename
            mapping[currentname] = value
            continue

        df = df.withColumnRenamed(currentname, rename)
        mapping[rename] = value

    # do all remaining stuffs
    for key, value in mapping.items():
        column_name = key
        isNew = not (column_name in df.columns)

        type = value.get("type", None)
        supported_types = [
            types.ByteType.typeName(),
            types.ShortType.typeName(),
            types.IntegerType.typeName(),
            types.LongType.typeName(),
            types.FloatType.typeName(),
            types.DoubleType.typeName(),
            types.DecimalType.typeName(),
            types.StringType.typeName(),
            types.BooleanType.typeName(),
            types.TimestampType.typeName(),
            types.DateType.typeName(),
            types.ArrayType.typeName(),
        ]

        from pyspark.sql import functions as F
        if (type is not None) and (not type in supported_types):
            raise ValueError("Type '{}' not supported!".format(type))

        if isNew:  # new column
            column_value = value.get("value", None)
            if column_value is None:
                raise ValueError("'value' is required for new column '{}'".format(column_name))
            df = df.withColumn(column_name, F.expr(column_value))

        if type is not None:  # casting
            df = df.withColumn(column_name, F.col(column_name).cast(type))

        if value.get("remove_tones", False):
            if df.schema[column_name].dataType != types.StringType():
                raise ValueError("'remove_tones' option works for 'string' column only!")
            # print("Removed tones:", currentname)
            df = df.withColumn(column_name, remove_tones_udf(F.col(column_name)))

        fillna = value.get("fillna", None)  # fill should be passed to expr() function
        if fillna is not None:
            # print("fillna: ", column_name, fillna)
            # df = df.fillna(fillna, subset=[column_name])
            df = df.fillna({column_name: fillna})

    return df
