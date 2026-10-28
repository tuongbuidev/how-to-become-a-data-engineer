"""Đọc batch JSON thời tiết và ghi dữ liệu phẳng ra Parquet bằng PySpark."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import arrays_zip, col, explode, input_file_name, size, sum as spark_sum
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# Khai báo schema giúp Spark đọc ổn định và tránh phải suy luận kiểu từ dữ liệu nhỏ.
WEATHER_SCHEMA = StructType(
    [
        StructField("source", StringType(), nullable=False),
        StructField("city_key", StringType(), nullable=False),
        StructField("city_name", StringType(), nullable=False),
        StructField("requested_date", StringType(), nullable=False),
        StructField("fetched_at", StringType(), nullable=False),
        StructField(
            "payload",
            StructType(
                [
                    StructField(
                        "daily",
                        StructType(
                            [
                                StructField("time", ArrayType(StringType()), nullable=False),
                                StructField(
                                    "temperature_2m_mean",
                                    ArrayType(DoubleType()),
                                    nullable=False,
                                ),
                                StructField(
                                    "precipitation_sum",
                                    ArrayType(DoubleType()),
                                    nullable=False,
                                ),
                                StructField(
                                    "wind_speed_10m_max",
                                    ArrayType(DoubleType()),
                                    nullable=False,
                                ),
                            ]
                        ),
                        nullable=False,
                    )
                ]
            ),
            nullable=False,
        ),
    ]
)


def transform_weather_batch(raw_df: DataFrame) -> DataFrame:
    """Trải các mảng daily thành một dòng cho mỗi thành phố và ngày quan sát."""
    # arrays_zip ghép các mảng theo cùng vị trí; explode biến mỗi ngày thành một dòng.
    daily_df = raw_df.select(
        "city_key",
        "city_name",
        "requested_date",
        explode(
            arrays_zip(
                col("payload.daily.time"),
                col("payload.daily.temperature_2m_mean"),
                col("payload.daily.precipitation_sum"),
                col("payload.daily.wind_speed_10m_max"),
            )
        ).alias("daily_values"),
    )

    return daily_df.select(
        "city_key",
        "city_name",
        "requested_date",
        col("daily_values.time").alias("observation_date"),
        col("daily_values.temperature_2m_mean").alias("mean_temperature_c"),
        col("daily_values.precipitation_sum").alias("precipitation_mm"),
        col("daily_values.wind_speed_10m_max").alias("max_wind_speed_kmh"),
    ).withColumn("source_file", input_file_name())


def main() -> None:
    """Đọc JSON, kiểm tra số dòng và ghi kết quả Parquet."""
    spark = SparkSession.builder.appName("weather-batch-to-parquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # File JSON của pipeline được format nhiều dòng; bật multiLine để đọc mỗi file thành một bản ghi.
    raw_df = (
        spark.read.option("multiLine", True)
        .schema(WEATHER_SCHEMA)
        .json("/data/input/*.json")
    )
    invalid_rows = raw_df.filter(
        (size(col("payload.daily.time")) != size(col("payload.daily.temperature_2m_mean")))
        | (size(col("payload.daily.time")) != size(col("payload.daily.precipitation_sum")))
        | (size(col("payload.daily.time")) != size(col("payload.daily.wind_speed_10m_max")))
    )
    if invalid_rows.limit(1).count() > 0:
        raise ValueError("Các mảng ngày và chỉ số thời tiết không cùng số phần tử")

    result_df = transform_weather_batch(raw_df)
    input_count = raw_df.count()
    expected_count = raw_df.select(
        spark_sum(size(col("payload.daily.time"))).alias("day_count")
    ).first()[0]
    actual_count = result_df.count()
    if actual_count != expected_count:
        raise ValueError(
            f"Số dòng đầu ra không khớp: expected={expected_count}, output={actual_count}"
        )

    result_df.orderBy("city_key").show(truncate=False)
    output_path = "/data/output/weather_daily"
    result_df.write.mode("overwrite").parquet(output_path)

    # Đọc lại Parquet để kiểm tra dữ liệu đã được ghi và có thể sử dụng tiếp.
    parquet_df = spark.read.parquet(output_path)
    parquet_count = parquet_df.count()
    if parquet_count != actual_count:
        raise ValueError(
            f"Số dòng Parquet không khớp: expected={actual_count}, actual={parquet_count}"
        )
    parquet_df.printSchema()
    print(
        f"Đã đọc {input_count} file, ghi và đọc lại {parquet_count} dòng Parquet "
        f"tại {output_path}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
