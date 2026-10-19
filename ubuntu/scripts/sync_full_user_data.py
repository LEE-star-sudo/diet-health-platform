"""把亚马逊 MySQL 里的用户全量覆盖写到本机 HDFS。

个人档案不包含 password_hash。饮食表按 MySQL 当前内容整表覆盖，
所以要在亚马逊已经写入全量之后再运行。
"""

import os

from pyspark.sql import SparkSession

JDBC_URL = (
    "jdbc:mysql://" + os.environ["MYSQL_HOST"] + ":3306/diet_health"
    "?useSSL=false&allowPublicKeyRetrieval=true"
    "&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL"
)
JDBC_PROPS = {
    "user": "health_admin",
    "password": os.environ["MYSQL_PASSWORD"],
    "driver": "com.mysql.cj.jdbc.Driver",
}
HDFS = "hdfs://192.168.164.128:9000"

# MySQL 表名 -> HDFS 目录。user_profile 会丢掉密码哈希。
TABLES = {
    "user_profile": f"{HDFS}/lakehouse/dim/user_profile",
    "food_nutrition_dict": f"{HDFS}/lakehouse/dim/food_nutrition_dict",
    "user_food_intake": f"{HDFS}/lakehouse/dwd/user_food_intake",
    "user_food_nutrition": f"{HDFS}/lakehouse/dwd/user_food_nutrition",
    "user_daily_summary": f"{HDFS}/lakehouse/dwd/user_daily_summary",
}


def main():
    spark = (
        SparkSession.builder.appName("diet-health-full-snapshot")
        .master("local[*]")
        .config("spark.hadoop.dfs.replication", "2")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for table, path in TABLES.items():
        frame = spark.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)
        if table == "user_profile":
            frame = frame.drop("password_hash")
        if table == "user_food_nutrition" and "timestamp" in frame.columns:
            frame = frame.withColumnRenamed("timestamp", "event_time")
        count = frame.count()
        frame.write.mode("overwrite").parquet(path)
        print(f"{table}: wrote {count} rows to {path}")

    spark.stop()


if __name__ == "__main__":
    main()
