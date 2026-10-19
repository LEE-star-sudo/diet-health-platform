# Ubuntu 数据湖

这是宿舍内网里的分析侧，主机 `192.168.164.128`，用户 `ccl`。亚马逊服务器上的打卡、营养换算、MySQL 和 Kafka 在仓库根目录。这边只拉取已经算好的结果，不在这里重算营养。

宿舍网络是内网，亚马逊连不进来。这台机器主动连接公网 `REDACTED_HOST`。

```text
手机
  → 亚马逊 meal-service
  → Kafka user_meal_event_stream
       ├→ MySQL user_food_intake
       └→ 换算后写入 user_food_nutrition，再发到 user_food_nutrition_stream
            → 亚马逊 Flink → MySQL user_daily_summary

宿舍 Ubuntu
  Flink SQL：两个 Kafka 主题 → HDFS 两张 ODS 流水表
  Spark：MySQL 五张表 → HDFS 五张全量表（手动执行）
  Hive：库 lakehouse_db，给这些 HDFS 目录起表名
```

## 目录

```text
ubuntu/
  flink/jobs/ods_kafka_to_hdfs.sql    订阅两个 Kafka 主题，写入 ODS
  flink/docker-compose.yml            Flink JobManager 和 TaskManager
  flink/docker-compose-hive.yml       HiveServer2
  flink/hive-entrypoint.sh
  flink/hadoop-conf/                  core-site、hdfs-site、hive-site
  hdfs/docker-compose.yml             NameNode
  hive/lakehouse_full.sql             Hive 外部表
  scripts/sync_full_user_data.py      Spark 从 MySQL 整表覆盖
  scripts/train_bmi_weight_regression.py
                                      用单日饮食做 BMI、体重截面回归，还没有产出模型
```

机器上正在跑的容器仍使用 `/home/ccl/flink-cluster` 和 `/home/ccl/hdfs-cluster`。这里是交给仓库的同一份配置，不包含 NameNode 数据、Hive 元数据和日志。

旧的 `kafka_to_hdfs_parquet.sql` 和 Flume 配置没有放进来，不要再提交那份旧作业。

## Flink：新打卡进入 ODS

| Kafka 主题 | 消费组 | HDFS |
|---|---|---|
| `user_meal_event_stream` | `lakehouse_ods_meal_event_v1` | `/lakehouse/ods/meal_event` |
| `user_food_nutrition_stream` | `lakehouse_ods_food_nutrition_v1` | `/lakehouse/ods/food_nutrition_event` |

`scan.startup.mode` 是 `latest-offset`，从提交那一刻的队列末尾往后读。提交前已经留在主题里的旧消息不会进 HDFS。

```bash
docker cp ubuntu/flink/jobs/ods_kafka_to_hdfs.sql flink_jobmanager:/tmp/ods_kafka_to_hdfs.sql
docker exec -e TERM=dumb flink_jobmanager ./bin/sql-client.sh -f /tmp/ods_kafka_to_hdfs.sql
```

在这台机器上，源文件的实际路径是 `/home/ccl/flink-cluster/jobs/ods_kafka_to_hdfs.sql`。

容器设置了 `restart: always`。开机只会拉起空集群，不会自动再交这份 SQL。作业停了之后，再次提交仍从当时的末尾开始，停机期间的新消息不会补进来。

## Spark：五张全量表

运行前设置 `MYSQL_PASSWORD`。脚本不把 `password_hash` 写入数据湖。`user_food_nutrition.timestamp` 写入湖时列名是 `event_time`。

```bash
export MYSQL_PASSWORD='数据库密码'
HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop \
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
/usr/local/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  ubuntu/scripts/sync_full_user_data.py
```

| MySQL | Hive 表 | HDFS |
|---|---|---|
| `user_profile` | `dim_user_profile` | `/lakehouse/dim/user_profile` |
| `food_nutrition_dict` | `dim_food_nutrition_dict` | `/lakehouse/dim/food_nutrition_dict` |
| `user_food_intake` | `dwd_user_food_intake` | `/lakehouse/dwd/user_food_intake` |
| `user_food_nutrition` | `dwd_user_food_nutrition` | `/lakehouse/dwd/user_food_nutrition` |
| `user_daily_summary` | `dwd_user_daily_summary` | `/lakehouse/dwd/user_daily_summary` |

个人档案和日汇总没有 Kafka 主题，只能靠这次整表读取。全量分析读 DIM、DWD 和日汇总。不要把 ODS 和 DWD 加在一起，同一顿饭会算两次。

2026-10-14 跑过一次：食物字典 3989 行，四张用户表 0 行，因为当时亚马逊上的用户数据已经清空。亚马逊重新写入全量之后，需要再执行一次。

## Hive

```bash
beeline -u 'jdbc:hive2://127.0.0.1:10000/lakehouse_db' -n ccl
```

表定义在 `hive/lakehouse_full.sql`。`dwd_user_daily_summary` 的内容是一个人一天的合计，表名仍放在 `dwd_` 下。没有单独的 DWS 或 ADS。

## 集群

| 组件 | 地址或容器 |
|---|---|
| HDFS NameNode | `hdfs://192.168.164.128:9000`，容器 `hdfs_namenode` |
| DataNode | `node2`（192.168.164.129）、`node3`（192.168.164.130），副本数 2 |
| Flink | `flink_jobmanager`、`flink_taskmanager`，页面 `http://127.0.0.1:8081` |
| HiveServer2 | `hive_server`，端口 `10000` |

调查用的 CSV 和 NHANES 2021–2023 的 XPT 留在机器的 `/home/ccl/data`，不放进这个仓库。
