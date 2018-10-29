import pyspark.sql.functions as F

def dataframe_diff(df_a, df_b, exclude_cols=[]):
    colnames_a = set(df_a.columns)
    colnames_b = set(df_a.columns)
    colnames = colnames_a & colnames_b

    c = colnames.difference(set(exclude_cols))
    colnames_a = [x for x in df_a.columns if x in c]
    colnames_b = [x for x in df_b.columns if x in c]

    #deleted
    df_a_min_b = df_a.select(colnames_a).subtract(df_b.select(colnames_b))

    # insert, modified
    df_b_min_a = df_b.select(colnames_b).subtract(df_a.select(colnames_a))

    return df_a_min_b, df_b_min_a
