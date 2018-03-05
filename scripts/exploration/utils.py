from pyspark.sql import DataFrame, SQLContext, SparkSession


def read_data_frame(filepath: str) -> DataFrame:
    return (SQLContext
            .getOrCreate(SparkSession.builder.getOrCreate())
            .read.parquet(filepath).cache())
