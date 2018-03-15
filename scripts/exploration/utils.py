import math

from matplotlib import ticker
from pyspark.sql import DataFrame, SQLContext, SparkSession

from exploration.patients_stats import millnames


def read_data_frame(filepath: str) -> DataFrame:
    return (SQLContext
            .getOrCreate(SparkSession.builder.getOrCreate())
            .read.parquet(filepath).cache())


millnames = ['', ' K', ' M', ' Mi', ' Tr']


@ticker.FuncFormatter
def millify(x, pos):
    x = float(x)
    millidx = max(0, min(len(millnames) - 1,
                         int(math.floor(0 if x == 0 else math.log10(abs(x)) / 3))))

    if millidx > 1:
        return '{:.1f}{}'.format(x / 10 ** (3 * millidx), millnames[millidx])
    else:
        return '{:.0f}{}'.format(x / 10 ** (3 * millidx), millnames[millidx])