[中文](#饮食健康平台) | [English](#diet-health-platform)

# 饮食健康平台

这个仓库是整个项目，包含两台机器。

亚马逊服务器负责实时链路：用户通过 HTTP 上报吃了什么和多少克，Spring Boot 写入 Kafka，换算营养后由 Flink 做按人按天的日汇总，结果放在 MySQL 和 Redis。宿舍里的 Ubuntu 负责数据湖：主动连接亚马逊，把同一份已经算好的数据拉进 Hadoop，供以后的 Spark 分析。营养只在亚马逊计算一次。

Ubuntu 侧的配置、拉取脚本和 Hive 表在 [`ubuntu/README.md`](ubuntu/README.md)。下面先写亚马逊服务器。服务器上的代码是多模块工程，不是单一 Maven 父项目。

## 系统能做什么

1. 接收用户上传的进食内容（食物编码、食物名称、食用克数、餐次）。
2. 把记录投递到 Kafka 主题 `user_meal_event_stream`。
3. **分支 A（明细）**：`meal-service` 的打卡消费者写入 `user_food_intake`。同一主题的换算消费者查食物字典后写入 `user_food_nutrition`。
4. **分支 B（汇总）**：Flink 作业按用户、自然日做 5 秒滚动窗口聚合，结果同时写入：
   - MySQL 表 `user_daily_summary`（累计 UPSERT）
   - Redis Hash `daily:summary:{userId}:{statDate}`（TTL 2 天，便于实时查询）

`data/` 里是 NHANES 2021–2023 的公开调查数据，只供学习、课程讨论和本项目的方法演示，不能当作医疗建议。来源和字段见 [`data/README.md`](data/README.md)。

## 整体架构

```
客户端 / 测试工具
        |
        |  POST /api/meal/record
        v
  meal-service (Spring Boot :8080)
        |
        |  produce
        v
  Kafka: user_meal_event_stream
        |
        +--> MealConsumer --> MySQL user_food_intake
        |
        +--> NutritionConsumer --> user_food_nutrition + nutrition_outbox
                                         |
                                         v
                           Kafka: user_food_nutrition_stream
                                         |
                                         v
                            NutritionDailySummaryJob
                               |                 |
                               v                 v
                    MySQL user_daily_summary   Redis daily:summary:*
```

## 目录结构

```
.
├── meal-service/                 Spring Boot 饮食上报与明细落库服务
│   ├── pom.xml
│   └── src/main/java/com/example/meal_service/
│       ├── MealServiceApplication.java
│       ├── MealController.java   POST /api/meal/record
│       ├── MealEvent.java        事件 DTO
│       ├── MealConsumer.java     Kafka 消费并写入 user_food_intake
│       ├── NutritionConsumer.java 另一消费者组，查字典后写入 user_food_nutrition
│       ├── NutritionConversionService.java 事务换算并建立发件箱事件
│       ├── NutritionOutboxPublisher.java 重试发布营养事件
│       ├── FoodIntakeEntity.java 对应表 user_food_intake
│       ├── FoodIntakeRepository.java
│       ├── FoodNutritionEntity.java 对应表 food_nutrition_dict
│       ├── FoodNutritionRepository.java
│       ├── MealRecordEntity.java 对应表 user_food_nutrition
│       └── MealRecordRepository.java
├── meal-flink-job/               Flink 日汇总实时作业
│   ├── pom.xml
│   └── src/main/java/com/meal/flink/NutritionDailySummaryJob.java
├── user-profile-service/          Spring Boot 用户档案服务
├── data/                          NHANES 公开数据，供学习讨论，见 data/README.md
├── scripts/data-import/           食物字典导入与补全脚本
├── docs/                          专题文档、压测计划和原始设计方案
├── start-flink-job.sh            等待 Flink 就绪后提交作业（防重复提交）
├── ubuntu/                        宿舍 Ubuntu 上的 Hadoop 数据湖
│   ├── flink/                     订阅 Kafka 写入 ODS，以及 Hive 容器
│   ├── hdfs/                      NameNode
│   ├── hive/lakehouse_full.sql    Hive 外部表
│   └── scripts/                   Spark 全量覆盖，以及尚未跑完的回归脚本
└── README.md                      项目总说明
```

## 模块说明

### 1. meal-service

- **技术栈**：Spring Boot 4.1.1、Java 17、Spring Web、Spring Kafka、Spring Data JPA、MySQL Connector
- **端口**：`8080`
- **Kafka**：`localhost:9092`
- **MySQL**：`jdbc:mysql://localhost:3306/diet_health`（用户 `root` / 密码见 `application.properties`）
- **JPA**：`spring.jpa.hibernate.ddl-auto=none`，表需事先建好

#### HTTP 接口

`POST /api/meal/record`

请求体示例：

```json
{
  "userId": "user_001",
  "timestamp": 1728691200000,
  "mealType": "Lunch",
  "foodCode": "11100000",
  "foodName": "米饭",
  "portionG": 200.0
}
```

- `userId`、`mealType`、`foodCode`、`foodName` 必填。
- `portionG` 必须大于 0。
- `timestamp` 为空时服务端填入当前毫秒时间戳。
- 服务端用 Jackson 生成 JSON 投递 Kafka，**分区 key 为 `userId`**。

当前写入原始 Kafka 主题的字段是：`userId`、`timestamp`、`mealType`、`foodCode`、`foodName`、`portionG`。营养换算完成后会通过事务发件箱把完整营养事件写到 `user_food_nutrition_stream`，Flink 只消费这个新主题。

#### Kafka 消费落库

`MealConsumer` 监听同一主题，消费者组 `meal_mysql_consumer_group`，并发度 3。字段齐全时写入 `user_food_intake`。毫秒时间戳按 UTC 转成 `record_time`。缺食物字段的旧消息会被跳过并记错误日志。

`NutritionConsumer` 用另一个消费者组 `meal_nutrition_consumer_group` 读同一主题。它按 `food_code` 查 `food_nutrition_dict`，用 `字典值 × portion_g / unit_weight_g` 写入 `user_food_nutrition` 的普通营养和 HEI 食物当量。补不上的食物构成写 `NULL`，并把 `pattern_data_available` 设为 0。营养行和 `nutrition_outbox` 事件在同一个 MySQL 事务中保存；后台发布器把事件可靠地发送到 `user_food_nutrition_stream`，失败时持续重试。

### 2. meal-flink-job

- **技术栈**：Apache Flink 1.17.2、Java 11、Flink Kafka Connector、Flink JDBC Connector、Jedis
- **主类**：`com.meal.flink.NutritionDailySummaryJob`
- **作业名**：`MealDailySummaryRealtimeAggregationJob`
- **主题**：`user_food_nutrition_stream`
- **消费者组**：`flink_nutrition_daily_summary_v1`
- **起始位点**：优先恢复已提交位点；第一次从 `earliest` 开始
- **时间**：消息事件时间，按 `Asia/Shanghai` 划分日期，允许 5 分钟乱序
- **状态**：按 `userId + statDate` 保存完整累计，并按 `nutritionId` 去重
- **容错**：每 10 秒 checkpoint

完整说明见 [`docs/FLINK_DAILY_SUMMARY_README.md`](docs/FLINK_DAILY_SUMMARY_README.md)。

#### MySQL Sink

对 `user_daily_summary` 做 `INSERT ... ON DUPLICATE KEY UPDATE`，每次覆盖当天完整累计值。要求表上有 `(user_id, stat_date)` 唯一键。

#### Redis Sink

写入 Hash：

```
daily:summary:{userId}:{yyyy-MM-dd}
```

字段包括 `total_calories`、`total_carbohydrate_g`、`total_protein_g`、`total_fat_g`、`total_sodium_mg`、`total_potassium_mg`、`meal_count`。过期时间 172800 秒（2 天）。连接地址硬编码为 `localhost:6379`。

Flink 作业目前把 Kafka / MySQL / Redis 都写成 `localhost`。本环境中 JobManager / TaskManager 使用 **host 网络**，因此容器内 `localhost` 可以访问宿主机上的 Kafka、MySQL、Redis。若改成 bridge 网络，需要同步改这些地址。

### 3. data/

`data/diet_foods_extracted.csv` 有 100116 行、6751 名受访者，一行是一种食物。字段包括受访者编号、年龄、性别、种族、BMI、体重、身高、腰围、进食钟点、餐次、USDA 食物编码、食物名、克数，以及这一份的热量、碳水、蛋白质、总脂肪、总糖和钠。编号 130378–142310 对应 NHANES 2021 年 8 月至 2023 年 8 月的公开数据。它不直接进入实时链路，用来导入档案和食物字典，也作为离线分析样本。

`data/nhanes/2021-2023/` 是同一周期从 CDC 下载的原始 XPT，包括体力活动、血压、血脂、血糖、糖化血红蛋白、睡眠、吸烟、饮酒和自报病史。官方链接和学习用途写在 [`data/README.md`](data/README.md)。

## 数据表（diet_health）

库里保留 5 张表：用户档案、吃了什么、营养指标、食物营养字典、Flink 日汇总。

### user_profile

用户账号和身体档案。手机注册由 `user-profile-service` 写入，`source_type` 为 `real`。CSV 里的 6751 名受访者由 `scripts/data-import/import_user_profiles.py` 写入，`user_id`、`account` 和 `nhanes_respondent_id` 都使用受访者编号，`source_type` 为 `nhanes`。调查数据没有活动量、目标和目标热量，这些列保持为空。

主键：`user_id`。`account` 唯一。

| 字段 | 类型 | 说明 |
|------|------|------|
| `user_id` | varchar(64) | 主键。手机注册由服务端生成；调查用户等于受访者编号 |
| `account` | varchar(64) | 登录账号，唯一 |
| `password_hash` | varchar(255) | PBKDF2-SHA256 密码哈希 |
| `display_name` | varchar(64) | 昵称 |
| `source_type` | varchar(32) | `real` 为手机注册用户，`nhanes` 为调查受访者 |
| `nhanes_respondent_id` | bigint | 演示受访者编号，真实用户为空 |
| `age` | int | 周岁 |
| `gender` | varchar(16) | `Male` 或 `Female` |
| `height_cm` | decimal(5,2) | 身高，厘米 |
| `weight_kg` | decimal(5,2) | 体重，公斤 |
| `waist_cm` | decimal(5,2) | 腰围，厘米 |
| `bmi` | decimal(4,2) | 体质指数 |
| `activity_level` | varchar(32) | `sedentary`、`light`、`moderate`。调查用户为空 |
| `pal_coefficient` | decimal(4,3) | 活动系数。调查用户为空 |
| `goal_type` | varchar(16) | `lose`、`maintain`、`gain`。调查用户为空 |
| `target_weight_kg` | decimal(5,2) | 目标体重，公斤 |
| `target_calories` | decimal(7,2) | 早期目标热量。当前建档和调查导入都不写此列 |
| `created_at` | timestamp | 创建时间 |
| `updated_at` | timestamp | 更新时间 |

### user_food_intake

用户实际上传的进食内容。一行是一种食物，记录吃了什么和多少克。`meal-service` 消费 Kafka 后写入本表。营养结果在 `user_food_nutrition`，用 `intake_id` 指回本表的 `id`。

主键：`id`。索引：`(user_id, record_time)`、`food_code`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | bigint | 自增主键 |
| `user_id` | varchar(64) | 用户 ID |
| `record_time` | datetime | 进食时间 |
| `meal_type` | varchar(32) | 早餐、午餐、晚餐或加餐 |
| `food_code` | varchar(64) | 食物编码，对应 `food_nutrition_dict.food_code` |
| `food_name` | varchar(128) | 上传时的食物名称 |
| `portion_g` | decimal(10,2) | 食用克数 |
| `created_at` | timestamp | 上传时间 |

### user_food_nutrition

一条打卡算出来的营养结果。一行对应 `user_food_intake` 的一行，用 `intake_id` 指回去。换算消费者写入普通营养、脂肪酸和 HEI 食物当量；食物构成未知时相关列为 `NULL`，不会用 0 冒充。

主键：`id`。索引：`(user_id, timestamp)`、`intake_id`。外键：`intake_id` → `user_food_intake.id`，删除打卡时把 `intake_id` 置空。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | bigint | 自增主键 |
| `user_id` | varchar(64) | 用户 ID |
| `intake_id` | bigint | 对应 `user_food_intake.id`。已有旧行先为空 |
| `timestamp` | bigint | 事件时间，毫秒 |
| `meal_type` | varchar(32) | 餐次 |
| `calories` | double | 热量，千卡。默认 0 |
| `carbohydrate_g` | double | 碳水，克。默认 0 |
| `protein_g` | double | 蛋白质，克。默认 0 |
| `total_fat_g` | double | 总脂肪，克。默认 0 |
| `total_sugar_g` | double | 总糖，克。默认 0 |
| `dietary_fiber_g` | double | 膳食纤维，克。默认 0 |
| `sodium` | double | 钠，毫克。默认 0 |
| `potassium` | double | 钾，毫克。默认 0 |
| `added_sugar_g` | double | 添加糖，克。未知时为空 |
| `saturated_fat_g` | double | 饱和脂肪，克。默认 0 |
| `monounsaturated_fat_g` | double | 单不饱和脂肪，克 |
| `polyunsaturated_fat_g` | double | 多不饱和脂肪，克 |
| `whole_grain_oz_eq` | double | 全谷物，盎司当量 |
| `refined_grain_oz_eq` | double | 精制谷物，盎司当量 |
| `total_vegetable_cup_eq` | double | 蔬菜总量，杯当量 |
| `dark_green_veg_beans_cup_eq` | double | 深色蔬菜和豆类，杯当量 |
| `total_fruit_cup_eq` | double | 水果总量，杯当量 |
| `whole_fruit_cup_eq` | double | 完整水果，杯当量 |
| `dairy_cup_eq` | double | 乳制品，杯当量 |
| `seafood_plant_protein_oz_eq` | double | 海产品或植物蛋白，盎司当量 |
| `pattern_data_available` | tinyint(1) | HEI 食物构成是否已知 |
| `unsaturated_saturated_fat_ratio` | double | 不饱和脂肪与饱和脂肪的比值。默认 0 |

### food_nutrition_dict

食物营养表。按食物编码保存每 100 克的营养素，供食物查询和换算。热量、碳水、蛋白质、总脂肪和钠来自 `data/diet_foods_extracted.csv`，由 `scripts/data-import/import_food_nutrition_dict.py` 按「营养合计 ÷ 克数合计 × 100」写入，共 3986 种 USDA 食物。钾、总糖、膳食纤维、饱和脂肪、单不饱和脂肪、多不饱和脂肪、胆固醇、维生素 A、C、D、E、K、硫胺素、核黄素、烟酸、维生素 B6、叶酸、维生素 B12、钙、铁、镁、锌、硒、咖啡因和酒精来自 FNDDS 2021–2023，文件在 `data/FNDDS_2021-2023_Nutrient_Values.xlsx`，由 `scripts/data-import/supplement_food_dict_from_fndds.py` 按食物编码补上。添加糖和 HEI 食物构成由 `scripts/data-import/supplement_food_patterns.py` 写入。优先用 FPED 2017–2020 年 3 月（`data/FPED_1720.xls`）按食物编码直接匹配，这一档已经是每 100 克。对不上的食物，用 FNDDS 2021–2023 食谱（`data/FNDDS_2021-2023_Ingredients.xlsx`）乘以 FPID 2017–2018 原料当量（`data/FPID_1718.xls`），再按成品重量折回每 100 克。FPED 2021–2023 还没有发布，所以这不是那一版的官方文件。3989 条字典里，3957 条直接匹配，16 条由食谱折算，16 条补不上，包括 `F1001`、`F1002`、`F1003`。补不上的成分留空，不用 0 代替。

主键：`id`。唯一键：`food_code`。索引：`food_name`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | bigint | 自增主键 |
| `food_code` | varchar(64) | 食物编码，唯一 |
| `food_name` | varchar(128) | 食物名称 |
| `unit_weight_g` | decimal(6,2) | 营养数值对应的重量，默认 100 克 |
| `energy_kcal` | decimal(6,2) | 该重量下的热量。默认 0 |
| `carbohydrate_g` | decimal(6,2) | 碳水，克 |
| `protein_g` | decimal(6,2) | 蛋白质，克 |
| `total_fat_g` | decimal(6,2) | 脂肪，克 |
| `total_sugar_g` | decimal(6,2) | 总糖，克。来自 FNDDS，每 100 克 |
| `dietary_fiber_g` | decimal(6,2) | 膳食纤维，克。来自 FNDDS，每 100 克 |
| `saturated_fat_g` | decimal(6,2) | 饱和脂肪，克。来自 FNDDS，每 100 克 |
| `monounsaturated_fat_g` | decimal(6,2) | 单不饱和脂肪，克。来自 FNDDS，每 100 克 |
| `polyunsaturated_fat_g` | decimal(6,2) | 多不饱和脂肪，克。来自 FNDDS，每 100 克 |
| `sodium_mg` | decimal(7,2) | 钠，毫克 |
| `potassium_mg` | decimal(7,2) | 钾，毫克。USDA 食物来自 FNDDS |
| `cholesterol_mg` | decimal(7,2) | 胆固醇，毫克。每 100 克 |
| `vitamin_a_rae_mcg` | decimal(7,2) | 维生素 A，微克 RAE。每 100 克 |
| `vitamin_c_mg` | decimal(7,2) | 维生素 C，毫克。每 100 克 |
| `vitamin_d_mcg` | decimal(7,2) | 维生素 D，微克。每 100 克 |
| `vitamin_e_mg` | decimal(7,2) | 维生素 E，毫克。每 100 克 |
| `vitamin_k_mcg` | decimal(7,2) | 维生素 K，微克。每 100 克 |
| `thiamin_mg` | decimal(7,2) | 硫胺素，维生素 B1，毫克。每 100 克 |
| `riboflavin_mg` | decimal(7,2) | 核黄素，维生素 B2，毫克。每 100 克 |
| `niacin_mg` | decimal(7,2) | 烟酸，维生素 B3，毫克。每 100 克 |
| `vitamin_b6_mg` | decimal(7,2) | 维生素 B6，毫克。每 100 克 |
| `folate_mcg` | decimal(7,2) | 叶酸总量，微克。每 100 克 |
| `vitamin_b12_mcg` | decimal(7,2) | 维生素 B12，微克。每 100 克 |
| `calcium_mg` | decimal(7,2) | 钙，毫克。每 100 克 |
| `iron_mg` | decimal(7,2) | 铁，毫克。每 100 克 |
| `magnesium_mg` | decimal(7,2) | 镁，毫克。每 100 克 |
| `zinc_mg` | decimal(7,2) | 锌，毫克。每 100 克 |
| `selenium_mcg` | decimal(7,2) | 硒，微克。每 100 克 |
| `caffeine_mg` | decimal(7,2) | 咖啡因，毫克。每 100 克 |
| `alcohol_g` | decimal(7,2) | 酒精，克。每 100 克 |
| `added_sugar_g` | decimal(8,2) | 添加糖，克。每 100 克。由茶匙当量 × 4.2 得到。空值表示未知 |
| `whole_grain_oz_eq` | decimal(10,4) | 全谷物，盎司当量。每 100 克 |
| `refined_grain_oz_eq` | decimal(10,4) | 精制谷物，盎司当量。每 100 克 |
| `total_vegetable_cup_eq` | decimal(10,4) | 蔬菜总量，杯当量。每 100 克。含豆类 |
| `dark_green_veg_beans_cup_eq` | decimal(10,4) | 深色蔬菜和豆类，杯当量。每 100 克 |
| `total_fruit_cup_eq` | decimal(10,4) | 水果总量，杯当量。每 100 克 |
| `whole_fruit_cup_eq` | decimal(10,4) | 完整水果，杯当量。每 100 克。水果总量减去果汁 |
| `dairy_cup_eq` | decimal(10,4) | 乳制品，杯当量。每 100 克 |
| `seafood_plant_protein_oz_eq` | decimal(10,4) | 海产品和植物蛋白，盎司当量。每 100 克。含鱼、大豆、坚果和豆类 |
| `fped_source` | varchar(32) | 来源。`fped_1720` 是直接匹配，`ingredient_rollup` 是食谱折算，`missing` 是补不上 |

### user_daily_summary

Flink 按用户、按天累计后的分析结果，使用 `INSERT ... ON DUPLICATE KEY UPDATE` 把窗口增量加进去。

主键：`id`。唯一键：`(user_id, stat_date)`。另有 `stat_date` 索引。

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | bigint | 自增主键 |
| `user_id` | varchar(64) | 用户 ID |
| `stat_date` | date | 汇总日期 |
| `total_calories` | decimal(12,2) | 累计热量，千卡 |
| `total_carbohydrate_g` | decimal(12,2) | 累计碳水，克 |
| `total_protein_g` | decimal(12,2) | 累计蛋白质，克 |
| `total_fat_g` | decimal(12,2) | 累计脂肪，克 |
| `total_sugar_g` | decimal(12,2) | 累计总糖，克 |
| `total_dietary_fiber_g` | decimal(12,2) | 累计膳食纤维，克 |
| `total_added_sugar_g` | decimal(12,2) | 累计添加糖，克 |
| `total_saturated_fat_g` | decimal(12,2) | 累计饱和脂肪，克 |
| `total_monounsaturated_fat_g` | decimal(12,2) | 累计单不饱和脂肪，克 |
| `total_polyunsaturated_fat_g` | decimal(12,2) | 累计多不饱和脂肪，克 |
| `total_sodium_mg` | decimal(14,2) | 累计钠，毫克 |
| `total_potassium_mg` | decimal(14,2) | 累计钾，毫克 |
| `total_*_oz_eq` / `total_*_cup_eq` | decimal(14,4) | HEI 食物构成的累计当量 |
| `meal_count` | int | 累计上报条数 |
| `unknown_pattern_count` | int | 当天食物构成未知的打卡数 |
| `is_calorie_surplus` | tinyint(1) | 是否热量盈余。默认 0。当前 Flink SQL 不更新此列 |
| `updated_at` | timestamp | 最近一次更新时间 |

## 健康指标怎么展示

手机上看到的健康数字分三层。打卡流水保存吃进去的营养素；Flink 保存每天累计值；供能比、能量差额、HEI 分数和预测是下一步计算的展示结果。

### 打卡时保存的营养素

营养指标保存在 `user_food_nutrition`。一行包含热量、碳水、蛋白质、总脂肪、总糖、膳食纤维、钠、钾，以及 HEI-2020 用的添加糖、饱和脂肪、全谷物、精制谷物、蔬菜、深色蔬菜和豆类、水果、完整水果、乳制品、海产品或植物蛋白、不饱和脂肪与饱和脂肪的比值。

当前换算会填写热量、宏量营养素、糖、纤维、脂肪酸、钠、钾、添加糖和全部已有的 HEI 食物当量。未知食物构成保持 `NULL`。

### 直接算出来就能展示的结果

这些不要再存进每一条打卡。把当天打卡加总，再读 `user_profile` 里的年龄、性别、身高、体重和活动系数即可。

| 展示结果 | 算法 | 合适的存放位置 |
|----------|------|----------------|
| 碳水供能比 | 碳水克数 × 4 ÷ 当日总热量。平衡区间 50%–65% | 当日汇总 |
| 蛋白质供能比 | 蛋白质克数 × 4 ÷ 当日总热量。平衡区间 10%–20% | 当日汇总 |
| 脂肪供能比 | 脂肪克数 × 9 ÷ 当日总热量。平衡区间 20%–30% | 当日汇总 |
| 钠钾摩尔比 | `(钠毫克 / 23) ÷ (钾毫克 / 39.1)`。健康阈值 ≤ 1 | 当日汇总 |
| 基础代谢 BMR | 男：`10×体重 + 6.25×身高 − 5×年龄 + 5`。女：最后一项为 −161 | 打开档案时现算，不入库 |
| 每日消耗 TDEE | `BMR × 活动系数`。久坐 1.2，轻度 1.375，中度 1.55 | 打开档案时现算，不入库 |
| 能量差额 | 当日总热量 − TDEE。正数是盈余，负数是缺口 | 当日汇总 |
| 未来 30 天、60 天 BMI | 用近 7 天或 14 天的平均热量，相对 TDEE 的差额按约 7700 千卡折合 1 公斤，并随体重变化修正消耗 | 分析报告，一个人一天一份 |
| 风险等级 | 模型输出 0 低危、1 中危、2 高危。输入是当日营养合计和档案体征 | 分析报告 |
| 前三项原因 | 模型给出贡献最高的三个因素和占比 | 分析报告 |
| 同龄人位置 | 把个人营养合计放到同性别、同年龄段人群里看百分位 | 分析报告。人群分布要单独有一份基准，不能从单次打卡里算出 |

`user_daily_summary` 已累计普通营养、脂肪酸、添加糖、HEI 食物当量和餐次。供能比、钠钾比、HEI 分数和能量差额还没有计算。

### 现在还展示不了的结果

HEI-2020 需要的食物成分已经放在 `food_nutrition_dict`、打卡营养行和日汇总中。分数本身还没有计算。

DII 还要看胆固醇、反式脂肪和多种维生素、矿物质。这些没有放进打卡表，所以炎症指数仍然留空。

## 运行时依赖

当前机器上已用 Docker 跑过：

| 组件 | 镜像 / 部署 | 端口 / 访问 |
|------|-------------|-------------|
| Kafka | `apache/kafka:latest`，容器名 `kafka` | `9092` |
| Redis | `redis:alpine`，容器名 `redis` | `6379` |
| Flink JobManager | `flink:1.17-scala_2.12`，`--network host` | Web UI `8081` |
| Flink TaskManager | 同上 | 与 JobManager 通信 |
| MySQL | 宿主机 `mysql-server` | `3306`，库 `diet_health` |

Kafka 主题：`user_meal_event_stream`。

## 如何启动

### 1. 基础设施

确保 Kafka、Redis、MySQL、Flink 已启动，并已创建主题与数据库表。

### 2. 启动 meal-service

```bash
cd /home/ubuntu/meal-service
./mvnw spring-boot:run
```

或使用已打包的 jar：

```bash
java -jar /home/ubuntu/meal-service/target/meal-service-0.0.1-SNAPSHOT.jar
```

### 3. 打包并提交 Flink 作业

```bash
cd /home/ubuntu/meal-flink-job
mvn -DskipTests package
/home/ubuntu/start-flink-job.sh
```

`start-flink-job.sh` 会：

1. 轮询 `http://localhost:8081/overview`，等待 Flink 就绪。
2. 若作业 `MealDailySummaryRealtimeAggregationJob` 已在运行则跳过。
3. 把 `meal-flink-job-1.0.0.jar` 拷进 JobManager 并 `flink run -d` 后台提交。

Flink Web UI：<http://localhost:8081>

### 4. 上报一条饮食记录

```bash
curl -X POST http://127.0.0.1:8080/api/meal/record \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user_001",
    "mealType": "Breakfast",
    "foodCode": "11100000",
    "foodName": "米饭",
    "portionG": 150
  }'
```

成功后：

- Kafka 主题出现一条 JSON。
- MySQL `user_food_intake` 增加一行。
- 若 `food_code` 在 `food_nutrition_dict` 中，`user_food_nutrition` 增加一行，并通过 `intake_id` 指回这条打卡。
- 营养事务发件箱发布成功后，Flink 消费营养主题并覆盖更新 MySQL 与 Redis 日汇总。

## 已知限制（当前实现）

- 添加糖和 HEI 食物构成直接匹配用的是 FPED 2017–2020 年 3 月，不是尚未发布的 FPED 2021–2023。食物构成未知时会增加 `unknown_pattern_count`，当天 HEI 不能视为完整结果。
- 字典里没有的食物编码不会生成营养行。没有对应打卡的旧营养行不会回填。
- 启动前要设置环境变量 `MYSQL_PASSWORD`。数据库地址仍写在配置文件中。
- Flink checkpoint 目前在容器文件系统；删除并重建容器前需要改成持久卷、HDFS 或 S3。
- HEI 最终分数、供能比、钠钾比、TDEE 和模型分析尚未实现。
- 工作区没有 Docker Compose、建表 SQL 和数据导入脚本的完整交付件。

## 技术栈一览

| 层 | 技术 |
|----|------|
| 接入服务 | Spring Boot 4.1.1、Java 17、Spring MVC、Spring Kafka、JPA |
| 消息队列 | Apache Kafka |
| 流计算 | Apache Flink 1.17.2 |
| 存储 | MySQL（明细 + 日汇总）、Redis（热汇总） |
| 离线数据 | `data/diet_foods_extracted.csv`（NHANES 风格饮食记录） |
| 数据湖 | 宿舍 Ubuntu 上的 HDFS、Hive、Spark，见 `ubuntu/` |


---

[中文](#饮食健康平台) | [English](#diet-health-platform)

# Diet Health Platform

This repository is the whole project and covers two machines.

The Amazon server runs the realtime path. A user reports what was eaten and how many grams over HTTP. Spring Boot writes that event to Kafka. After the nutrients are calculated, Flink builds a per-person, per-day summary and stores it in MySQL and Redis. The Ubuntu machine in the dorm runs the data lake. It connects out to Amazon and pulls the already calculated data into Hadoop for later Spark analysis. Nutrition is calculated once, on Amazon.

The Ubuntu configuration, pull scripts, and Hive tables are in [`ubuntu/README.md`](ubuntu/README.md). The Amazon server is described first. Its code is a multi-module workspace, not one Maven parent project.

## What the system does

1. Accepts an uploaded intake: food code, food name, grams, and meal.
2. Publishes the record to the Kafka topic `user_meal_event_stream`.
3. **Branch A (detail):** the check-in consumer in `meal-service` writes `user_food_intake`. A second consumer of the same topic looks up the food dictionary and writes `user_food_nutrition`.
4. **Branch B (summary):** the Flink job aggregates by user and calendar day in a 5-second tumbling window and writes both:
   - MySQL table `user_daily_summary` (cumulative UPSERT)
   - Redis hash `daily:summary:{userId}:{statDate}` (TTL 2 days, for realtime reads)

`data/` holds public NHANES 2021–2023 survey files for learning, classroom discussion, and method demonstrations in this project. They are not medical advice. Sources and columns are in [`data/README.md`](data/README.md).

## Architecture

```
Client / test tool
        |
        |  POST /api/meal/record
        v
  meal-service (Spring Boot :8080)
        |
        |  produce
        v
  Kafka: user_meal_event_stream
        |
        +--> MealConsumer --> MySQL user_food_intake
        |
        +--> NutritionConsumer --> user_food_nutrition + nutrition_outbox
                                         |
                                         v
                           Kafka: user_food_nutrition_stream
                                         |
                                         v
                            NutritionDailySummaryJob
                               |                 |
                               v                 v
                    MySQL user_daily_summary   Redis daily:summary:*
```

## Layout

```
.
├── meal-service/                 Spring Boot intake and detail service
│   ├── pom.xml
│   └── src/main/java/com/example/meal_service/
│       ├── MealServiceApplication.java
│       ├── MealController.java   POST /api/meal/record
│       ├── MealEvent.java        event DTO
│       ├── MealConsumer.java     Kafka consumer writing user_food_intake
│       ├── NutritionConsumer.java  other group; writes user_food_nutrition after a dictionary lookup
│       ├── NutritionConversionService.java  transactional conversion and outbox event
│       ├── NutritionOutboxPublisher.java  retries publication of nutrition events
│       ├── FoodIntakeEntity.java  table user_food_intake
│       ├── FoodIntakeRepository.java
│       ├── FoodNutritionEntity.java  table food_nutrition_dict
│       ├── FoodNutritionRepository.java
│       ├── MealRecordEntity.java  table user_food_nutrition
│       └── MealRecordRepository.java
├── meal-flink-job/               Flink daily-summary job
│   ├── pom.xml
│   └── src/main/java/com/meal/flink/NutritionDailySummaryJob.java
├── user-profile-service/          Spring Boot profile service
├── data/                          Public NHANES files for learning and discussion; see data/README.md
├── scripts/data-import/           Food-dictionary import and completion scripts
├── docs/                          Topic notes, load-test plan, and the original design
├── start-flink-job.sh            Waits until Flink is ready, then submits the job once
├── ubuntu/                        Hadoop lakehouse on the dorm Ubuntu machine
│   ├── flink/                     Kafka-to-ODS subscription, plus the Hive container
│   ├── hdfs/                      NameNode
│   ├── hive/lakehouse_full.sql    Hive external tables
│   └── scripts/                   Spark full overwrite, and a regression script that has not produced a model
└── README.md                      This document
```

## Modules

### 1. meal-service

- **Stack:** Spring Boot 4.1.1, Java 17, Spring Web, Spring Kafka, Spring Data JPA, MySQL Connector
- **Port:** `8080`
- **Kafka:** `localhost:9092`
- **MySQL:** `jdbc:mysql://localhost:3306/diet_health` (user `root`; password is supplied outside the committed files)
- **JPA:** `spring.jpa.hibernate.ddl-auto=none`. Create the tables before startup.

#### HTTP API

`POST /api/meal/record`

Example body:

```json
{
  "userId": "user_001",
  "timestamp": 1728691200000,
  "mealType": "Lunch",
  "foodCode": "11100000",
  "foodName": "米饭",
  "portionG": 200.0
}
```

- `userId`, `mealType`, `foodCode`, and `foodName` are required.
- `portionG` must be greater than 0.
- An empty `timestamp` is filled with the current time in milliseconds.
- The server publishes JSON with Jackson. **The partition key is `userId`.**

The raw topic currently carries `userId`, `timestamp`, `mealType`, `foodCode`, `foodName`, and `portionG`. After conversion, the transactional outbox publishes the full nutrition event to `user_food_nutrition_stream`. Flink consumes only that topic.

#### Kafka consumers

`MealConsumer` listens on the raw topic in group `meal_mysql_consumer_group` with concurrency 3. A complete message is written to `user_food_intake`. The millisecond timestamp is stored as `record_time` in UTC. Older messages that lack food fields are skipped and logged.

`NutritionConsumer` uses a different group, `meal_nutrition_consumer_group`, on the same topic. It looks up `food_nutrition_dict` by `food_code` and writes ordinary nutrients and HEI food equivalents to `user_food_nutrition` as `dictionary value × portion_g / unit_weight_g`. Missing food-pattern components stay `NULL`, and `pattern_data_available` is set to 0. The nutrition row and the `nutrition_outbox` event are saved in one MySQL transaction. A background publisher sends the event to `user_food_nutrition_stream` and keeps retrying after a failure.

### 2. meal-flink-job

- **Stack:** Apache Flink 1.17.2, Java 11, Flink Kafka Connector, Flink JDBC Connector, Jedis
- **Main class:** `com.meal.flink.NutritionDailySummaryJob`
- **Job name:** `MealDailySummaryRealtimeAggregationJob`
- **Topic:** `user_food_nutrition_stream`
- **Consumer group:** `flink_nutrition_daily_summary_v1`
- **Start offset:** resume a committed offset when one exists; the first run starts at `earliest`
- **Time:** event time, split into dates in `Asia/Shanghai`, with 5 minutes of allowed lateness
- **State:** full running totals keyed by `userId + statDate`, deduplicated by `nutritionId`
- **Fault tolerance:** a checkpoint every 10 seconds

The longer note is [`docs/FLINK_DAILY_SUMMARY_README.md`](docs/FLINK_DAILY_SUMMARY_README.md).

#### MySQL sink

`user_daily_summary` is written with `INSERT ... ON DUPLICATE KEY UPDATE`, replacing that day's full totals. The table needs a unique key on `(user_id, stat_date)`.

#### Redis sink

The hash key is:

```
daily:summary:{userId}:{yyyy-MM-dd}
```

Fields include `total_calories`, `total_carbohydrate_g`, `total_protein_g`, `total_fat_g`, `total_sodium_mg`, `total_potassium_mg`, and `meal_count`. The TTL is 172800 seconds (2 days). The connector address is hard-coded as `localhost:6379`.

The Flink job currently uses `localhost` for Kafka, MySQL, and Redis. In this environment the JobManager and TaskManager use **host networking**, so `localhost` inside the container reaches Kafka, MySQL, and Redis on the host. A bridge network would require those addresses to change together.

### 3. data/

`data/diet_foods_extracted.csv` has 100,116 rows and 6,751 respondents. One row is one food. Columns are respondent id, age, sex, race or ethnicity, BMI, weight, height, waist, clock time, meal, USDA food code, food name, grams, and that portion's energy, carbohydrate, protein, total fat, total sugar, and sodium. Respondent ids 130378–142310 are the public NHANES release for August 2021 through August 2023. The file is not on the realtime path. It is used to import profiles and the food dictionary, and it is also an offline analysis sample.

`data/nhanes/2021-2023/` holds the original XPT files downloaded from the CDC for the same cycle: physical activity, blood pressure, lipids, glucose, glycated hemoglobin, sleep, smoking, alcohol, and self-reported medical conditions. Official links and the learning-use note are in [`data/README.md`](data/README.md).

## Tables (diet_health)

Five tables remain: profile, what was eaten, calculated nutrients, the food dictionary, and the Flink daily summary.

### user_profile

Account and body profile. Phone registration is written by `user-profile-service` with `source_type` `real`. The 6,751 survey respondents in the CSV are written by `scripts/data-import/import_user_profiles.py`. For them, `user_id`, `account`, and `nhanes_respondent_id` are the respondent id, and `source_type` is `nhanes`. The survey has no activity, goal, or target calories, so those columns stay empty.

Primary key: `user_id`. `account` is unique.

| Column | Type | Meaning |
|------|------|------|
| `user_id` | varchar(64) | Primary key. Generated for a phone registration; equal to the respondent id for a survey user |
| `account` | varchar(64) | Login name, unique |
| `password_hash` | varchar(255) | PBKDF2-SHA256 password hash |
| `display_name` | varchar(64) | Display name |
| `source_type` | varchar(32) | `real` for a phone user, `nhanes` for a survey respondent |
| `nhanes_respondent_id` | bigint | Respondent id for a survey row; empty for a real user |
| `age` | int | Age in years |
| `gender` | varchar(16) | `Male` or `Female` |
| `height_cm` | decimal(5,2) | Height in centimeters |
| `weight_kg` | decimal(5,2) | Weight in kilograms |
| `waist_cm` | decimal(5,2) | Waist in centimeters |
| `bmi` | decimal(4,2) | Body mass index |
| `activity_level` | varchar(32) | `sedentary`, `light`, or `moderate`. Empty for survey users |
| `pal_coefficient` | decimal(4,3) | Physical activity level. Empty for survey users |
| `goal_type` | varchar(16) | `lose`, `maintain`, or `gain`. Empty for survey users |
| `target_weight_kg` | decimal(5,2) | Target weight in kilograms |
| `target_calories` | decimal(7,2) | Earlier target calories. Current registration and survey import do not write this column |
| `created_at` | timestamp | Created at |
| `updated_at` | timestamp | Updated at |

### user_food_intake

What the user actually uploaded. One row is one food: what was eaten and how many grams. `meal-service` writes it after consuming Kafka. The nutrient result is in `user_food_nutrition`, and `intake_id` points back to this table's `id`.

Primary key: `id`. Indexes: `(user_id, record_time)`, `food_code`.

| Column | Type | Meaning |
|------|------|------|
| `id` | bigint | Autoincrement primary key |
| `user_id` | varchar(64) | User id |
| `record_time` | datetime | Eating time |
| `meal_type` | varchar(32) | Breakfast, lunch, dinner, or snack |
| `food_code` | varchar(64) | Food code, matching `food_nutrition_dict.food_code` |
| `food_name` | varchar(128) | Food name at upload time |
| `portion_g` | decimal(10,2) | Grams eaten |
| `created_at` | timestamp | Upload time |

### user_food_nutrition

Nutrients calculated for one check-in. One row matches one `user_food_intake` row through `intake_id`. The conversion consumer writes ordinary nutrients, fatty acids, and HEI food equivalents. Unknown pattern components stay `NULL` and are not stored as 0.

Primary key: `id`. Indexes: `(user_id, timestamp)`, `intake_id`. Foreign key: `intake_id` → `user_food_intake.id`. Deleting a check-in sets `intake_id` to null.

| Column | Type | Meaning |
|------|------|------|
| `id` | bigint | Autoincrement primary key |
| `user_id` | varchar(64) | User id |
| `intake_id` | bigint | Matching `user_food_intake.id`. Older rows may be empty |
| `timestamp` | bigint | Event time in milliseconds |
| `meal_type` | varchar(32) | Meal |
| `calories` | double | Energy in kilocalories. Default 0 |
| `carbohydrate_g` | double | Carbohydrate in grams. Default 0 |
| `protein_g` | double | Protein in grams. Default 0 |
| `total_fat_g` | double | Total fat in grams. Default 0 |
| `total_sugar_g` | double | Total sugar in grams. Default 0 |
| `dietary_fiber_g` | double | Fiber in grams. Default 0 |
| `sodium` | double | Sodium in milligrams. Default 0 |
| `potassium` | double | Potassium in milligrams. Default 0 |
| `added_sugar_g` | double | Added sugar in grams. Empty when unknown |
| `saturated_fat_g` | double | Saturated fat in grams. Default 0 |
| `monounsaturated_fat_g` | double | Monounsaturated fat in grams |
| `polyunsaturated_fat_g` | double | Polyunsaturated fat in grams |
| `whole_grain_oz_eq` | double | Whole grains, ounce equivalents |
| `refined_grain_oz_eq` | double | Refined grains, ounce equivalents |
| `total_vegetable_cup_eq` | double | Total vegetables, cup equivalents |
| `dark_green_veg_beans_cup_eq` | double | Dark-green vegetables and beans, cup equivalents |
| `total_fruit_cup_eq` | double | Total fruit, cup equivalents |
| `whole_fruit_cup_eq` | double | Whole fruit, cup equivalents |
| `dairy_cup_eq` | double | Dairy, cup equivalents |
| `seafood_plant_protein_oz_eq` | double | Seafood or plant protein, ounce equivalents |
| `pattern_data_available` | tinyint(1) | Whether the HEI food pattern is known |
| `unsaturated_saturated_fat_ratio` | double | Unsaturated-to-saturated fat ratio. Default 0 |

### food_nutrition_dict

Nutrients per food code, stored per 100 grams, for lookup and conversion. Energy, carbohydrate, protein, total fat, and sodium come from `data/diet_foods_extracted.csv`. `scripts/data-import/import_food_nutrition_dict.py` writes them as `nutrient total ÷ gram total × 100` for 3,986 USDA foods. Potassium, total sugar, fiber, saturated fat, monounsaturated fat, polyunsaturated fat, cholesterol, vitamins A, C, D, E, and K, thiamin, riboflavin, niacin, vitamin B6, folate, vitamin B12, calcium, iron, magnesium, zinc, selenium, caffeine, and alcohol come from FNDDS 2021–2023 (`data/FNDDS_2021-2023_Nutrient_Values.xlsx`) through `scripts/data-import/supplement_food_dict_from_fndds.py`, matched by food code. Added sugar and HEI food-pattern equivalents are written by `scripts/data-import/supplement_food_patterns.py`. The first choice is a direct match to FPED for 2017 through March 2020 (`data/FPED_1720.xls`), which is already per 100 grams. Foods that do not match are rolled up from the FNDDS 2021–2023 recipe (`data/FNDDS_2021-2023_Ingredients.xlsx`) and FPID 2017–2018 ingredient equivalents (`data/FPID_1718.xls`), then scaled back to 100 grams of the finished food. FPED 2021–2023 has not been published, so this is not that official file. Of 3,989 dictionary rows, 3,957 match directly, 16 are recipe rollups, and 16 cannot be completed, including `F1001`, `F1002`, and `F1003`. Missing components stay empty. They are not replaced with 0.

Primary key: `id`. Unique key: `food_code`. Index: `food_name`.

| Column | Type | Meaning |
|------|------|------|
| `id` | bigint | Autoincrement primary key |
| `food_code` | varchar(64) | Food code, unique |
| `food_name` | varchar(128) | Food name |
| `unit_weight_g` | decimal(6,2) | Weight the nutrient values refer to, default 100 grams |
| `energy_kcal` | decimal(6,2) | Energy for that weight. Default 0 |
| `carbohydrate_g` | decimal(6,2) | Carbohydrate, grams |
| `protein_g` | decimal(6,2) | Protein, grams |
| `total_fat_g` | decimal(6,2) | Fat, grams |
| `total_sugar_g` | decimal(6,2) | Total sugar, grams. From FNDDS, per 100 grams |
| `dietary_fiber_g` | decimal(6,2) | Fiber, grams. From FNDDS, per 100 grams |
| `saturated_fat_g` | decimal(6,2) | Saturated fat, grams. From FNDDS, per 100 grams |
| `monounsaturated_fat_g` | decimal(6,2) | Monounsaturated fat, grams. From FNDDS, per 100 grams |
| `polyunsaturated_fat_g` | decimal(6,2) | Polyunsaturated fat, grams. From FNDDS, per 100 grams |
| `sodium_mg` | decimal(7,2) | Sodium, milligrams |
| `potassium_mg` | decimal(7,2) | Potassium, milligrams. USDA foods come from FNDDS |
| `cholesterol_mg` | decimal(7,2) | Cholesterol, milligrams. Per 100 grams |
| `vitamin_a_rae_mcg` | decimal(7,2) | Vitamin A, micrograms RAE. Per 100 grams |
| `vitamin_c_mg` | decimal(7,2) | Vitamin C, milligrams. Per 100 grams |
| `vitamin_d_mcg` | decimal(7,2) | Vitamin D, micrograms. Per 100 grams |
| `vitamin_e_mg` | decimal(7,2) | Vitamin E, milligrams. Per 100 grams |
| `vitamin_k_mcg` | decimal(7,2) | Vitamin K, micrograms. Per 100 grams |
| `thiamin_mg` | decimal(7,2) | Thiamin, vitamin B1, milligrams. Per 100 grams |
| `riboflavin_mg` | decimal(7,2) | Riboflavin, vitamin B2, milligrams. Per 100 grams |
| `niacin_mg` | decimal(7,2) | Niacin, vitamin B3, milligrams. Per 100 grams |
| `vitamin_b6_mg` | decimal(7,2) | Vitamin B6, milligrams. Per 100 grams |
| `folate_mcg` | decimal(7,2) | Total folate, micrograms. Per 100 grams |
| `vitamin_b12_mcg` | decimal(7,2) | Vitamin B12, micrograms. Per 100 grams |
| `calcium_mg` | decimal(7,2) | Calcium, milligrams. Per 100 grams |
| `iron_mg` | decimal(7,2) | Iron, milligrams. Per 100 grams |
| `magnesium_mg` | decimal(7,2) | Magnesium, milligrams. Per 100 grams |
| `zinc_mg` | decimal(7,2) | Zinc, milligrams. Per 100 grams |
| `selenium_mcg` | decimal(7,2) | Selenium, micrograms. Per 100 grams |
| `caffeine_mg` | decimal(7,2) | Caffeine, milligrams. Per 100 grams |
| `alcohol_g` | decimal(7,2) | Alcohol, grams. Per 100 grams |
| `added_sugar_g` | decimal(8,2) | Added sugar, grams. Per 100 grams. Teaspoon equivalents × 4.2. Empty means unknown |
| `whole_grain_oz_eq` | decimal(10,4) | Whole grains, ounce equivalents. Per 100 grams |
| `refined_grain_oz_eq` | decimal(10,4) | Refined grains, ounce equivalents. Per 100 grams |
| `total_vegetable_cup_eq` | decimal(10,4) | Total vegetables, cup equivalents. Per 100 grams. Includes beans |
| `dark_green_veg_beans_cup_eq` | decimal(10,4) | Dark-green vegetables and beans, cup equivalents. Per 100 grams |
| `total_fruit_cup_eq` | decimal(10,4) | Total fruit, cup equivalents. Per 100 grams |
| `whole_fruit_cup_eq` | decimal(10,4) | Whole fruit, cup equivalents. Per 100 grams. Total fruit minus juice |
| `dairy_cup_eq` | decimal(10,4) | Dairy, cup equivalents. Per 100 grams |
| `seafood_plant_protein_oz_eq` | decimal(10,4) | Seafood and plant protein, ounce equivalents. Per 100 grams. Includes fish, soy, nuts, and beans |
| `fped_source` | varchar(32) | Source. `fped_1720` is a direct match, `ingredient_rollup` is a recipe rollup, `missing` could not be completed |

### user_daily_summary

Flink's per-user, per-day totals. `INSERT ... ON DUPLICATE KEY UPDATE` adds each window's increment.

Primary key: `id`. Unique key: `(user_id, stat_date)`. There is also an index on `stat_date`.

| Column | Type | Meaning |
|------|------|------|
| `id` | bigint | Autoincrement primary key |
| `user_id` | varchar(64) | User id |
| `stat_date` | date | Summary date |
| `total_calories` | decimal(12,2) | Cumulative energy, kilocalories |
| `total_carbohydrate_g` | decimal(12,2) | Cumulative carbohydrate, grams |
| `total_protein_g` | decimal(12,2) | Cumulative protein, grams |
| `total_fat_g` | decimal(12,2) | Cumulative fat, grams |
| `total_sugar_g` | decimal(12,2) | Cumulative total sugar, grams |
| `total_dietary_fiber_g` | decimal(12,2) | Cumulative fiber, grams |
| `total_added_sugar_g` | decimal(12,2) | Cumulative added sugar, grams |
| `total_saturated_fat_g` | decimal(12,2) | Cumulative saturated fat, grams |
| `total_monounsaturated_fat_g` | decimal(12,2) | Cumulative monounsaturated fat, grams |
| `total_polyunsaturated_fat_g` | decimal(12,2) | Cumulative polyunsaturated fat, grams |
| `total_sodium_mg` | decimal(14,2) | Cumulative sodium, milligrams |
| `total_potassium_mg` | decimal(14,2) | Cumulative potassium, milligrams |
| `total_*_oz_eq` / `total_*_cup_eq` | decimal(14,4) | Cumulative HEI food-pattern equivalents |
| `meal_count` | int | Cumulative number of reported rows |
| `unknown_pattern_count` | int | Check-ins that day whose food pattern was unknown |
| `is_calorie_surplus` | tinyint(1) | Whether energy intake exceeded expenditure. Default 0. The current Flink SQL does not update this column |
| `updated_at` | timestamp | Last update time |

## How health figures are shown

The numbers on the phone have three layers. The check-in log stores nutrients eaten. Flink stores the day's running totals. Energy shares, the energy gap, the HEI score, and predictions are display results for a later step.

### Nutrients saved at check-in

Nutrients live in `user_food_nutrition`. One row has energy, carbohydrate, protein, total fat, total sugar, fiber, sodium, potassium, and the HEI-2020 inputs: added sugar, saturated fat, whole grains, refined grains, vegetables, dark-green vegetables and beans, fruit, whole fruit, dairy, seafood or plant protein, and the unsaturated-to-saturated fat ratio.

The current conversion fills energy, macronutrients, sugar, fiber, fatty acids, sodium, potassium, added sugar, and the HEI food equivalents that are available. An unknown food pattern stays `NULL`.

### Figures that can be calculated for display

Do not store these on every check-in. Sum the day's check-ins, then read age, sex, height, weight, and the activity coefficient from `user_profile`.

| Display | Formula | Where it belongs |
|----------|------|----------------|
| Carbohydrate energy share | carbohydrate grams × 4 ÷ day's total energy. Balance range 50%–65% | Daily summary |
| Protein energy share | protein grams × 4 ÷ day's total energy. Balance range 10%–20% | Daily summary |
| Fat energy share | fat grams × 9 ÷ day's total energy. Balance range 20%–30% | Daily summary |
| Sodium-to-potassium molar ratio | `(sodium mg / 23) ÷ (potassium mg / 39.1)`. Healthy threshold ≤ 1 | Daily summary |
| BMR | Men: `10×weight + 6.25×height − 5×age + 5`. Women: the last term is −161 | Computed when the profile is opened, not stored |
| TDEE | `BMR × activity coefficient`. Sedentary 1.2, light 1.375, moderate 1.55 | Computed when the profile is opened, not stored |
| Energy gap | Day's total energy − TDEE. Positive is a surplus, negative is a deficit | Daily summary |
| BMI 30 and 60 days ahead | Use the average energy of the last 7 or 14 days. The gap versus TDEE is about 7,700 kilocalories per kilogram, and expenditure is updated as weight changes | Analysis report, one copy per person per day |
| Risk level | Model output 0 low, 1 medium, 2 high. Inputs are the day's nutrient totals and the profile | Analysis report |
| Top three reasons | The three factors with the highest contribution, and their shares | Analysis report |
| Peer position | Place the person's nutrient totals in the percentile of the same sex and age band | Analysis report. The population distribution needs its own reference and cannot be computed from one check-in |

`user_daily_summary` already totals ordinary nutrients, fatty acids, added sugar, HEI food equivalents, and meal count. Energy shares, the sodium-to-potassium ratio, the HEI score, and the energy gap are not calculated yet.

### Figures that still cannot be shown

The food components required by HEI-2020 are already in `food_nutrition_dict`, the check-in nutrition row, and the daily summary. The score itself is not calculated.

DII also needs cholesterol, trans fat, and several vitamins and minerals. Those are not copied onto the check-in table, so the inflammation index stays empty.

## Runtime dependencies

These have been run with Docker on the server:

| Component | Image / deployment | Port / access |
|------|-------------|-------------|
| Kafka | `apache/kafka:latest`, container `kafka` | `9092` |
| Redis | `redis:alpine`, container `redis` | `6379` |
| Flink JobManager | `flink:1.17-scala_2.12`, `--network host` | Web UI `8081` |
| Flink TaskManager | same image | talks to the JobManager |
| MySQL | host `mysql-server` | `3306`, database `diet_health` |

Kafka topic: `user_meal_event_stream`.

## How to start

### 1. Infrastructure

Kafka, Redis, MySQL, and Flink must be running, and the topic and tables must exist.

### 2. Start meal-service

```bash
cd /home/ubuntu/meal-service
./mvnw spring-boot:run
```

Or use a built jar:

```bash
java -jar /home/ubuntu/meal-service/target/meal-service-0.0.1-SNAPSHOT.jar
```

### 3. Package and submit the Flink job

```bash
cd /home/ubuntu/meal-flink-job
mvn -DskipTests package
/home/ubuntu/start-flink-job.sh
```

`start-flink-job.sh`:

1. Polls `http://localhost:8081/overview` until Flink is ready.
2. Skips submission when `MealDailySummaryRealtimeAggregationJob` is already running.
3. Copies `meal-flink-job-1.0.0.jar` into the JobManager and submits it in the background with `flink run -d`.

Flink Web UI: <http://localhost:8081>

### 4. Submit one intake

```bash
curl -X POST http://127.0.0.1:8080/api/meal/record \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user_001",
    "mealType": "Breakfast",
    "foodCode": "11100000",
    "foodName": "米饭",
    "portionG": 150
  }'
```

After a successful call:

- One JSON message appears on the Kafka topic.
- MySQL `user_food_intake` gains one row.
- If `food_code` is in `food_nutrition_dict`, `user_food_nutrition` gains one row and `intake_id` points back to that check-in.
- After the nutrition outbox publishes, Flink consumes the nutrition topic and overwrites the MySQL and Redis daily summaries.

## Known limits of the current implementation

- Direct matches for added sugar and HEI food patterns use FPED for 2017 through March 2020, not the unpublished FPED 2021–2023. An unknown pattern increments `unknown_pattern_count`, and that day's HEI cannot be treated as complete.
- A food code missing from the dictionary does not create a nutrition row. Old nutrition rows without a matching check-in are not backfilled.
- Set `MYSQL_PASSWORD` before startup. Database addresses remain in the configuration files.
- Flink checkpoints are currently on the container filesystem. Move them to a persistent volume, HDFS, or S3 before deleting and recreating the containers.
- The final HEI score, energy shares, the sodium-to-potassium ratio, TDEE, and model analysis are not implemented.
- The workspace does not yet ship a complete set of Docker Compose files, table DDL, and data-import scripts for the server side.

## Stack

| Layer | Technology |
|----|------|
| Ingestion | Spring Boot 4.1.1, Java 17, Spring MVC, Spring Kafka, JPA |
| Queue | Apache Kafka |
| Stream processing | Apache Flink 1.17.2 |
| Storage | MySQL (detail and daily summary), Redis (hot summary) |
| Offline data | `data/diet_foods_extracted.csv` and the NHANES XPT files, for learning and discussion |
| Data lake | HDFS, Hive, and Spark on the dorm Ubuntu machine; see `ubuntu/` |
