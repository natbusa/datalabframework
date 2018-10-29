from pyspark.sql import types
from pyspark.sql.functions import pandas_udf, PandasUDFType


def remove_tones(s):
    intab  = "àáảãạăắằẵặẳâầấậẫẩđèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵÀÁẢÃẠĂẮẰẴẶẲÂẦẤẬẪẨĐÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴ"
    outtab = "aaaaaaaaaaaaaaaaadeeeeeeeeeeeiiiiiooooooooooooooooouuuuuuuuuuuyyyyyAAAAAAAAAAAAAAAAADEEEEEEEEEEEIIIIIOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYY"
    # return s if not s else s.translate(str.maketrans(intab, outtab))
    return s if not s else s.translate(str.maketrans(intab, outtab)).lower()

@pandas_udf(types.StringType(), PandasUDFType.SCALAR)
def remove_tones_udf(series):
    return series.apply(remove_tones)
