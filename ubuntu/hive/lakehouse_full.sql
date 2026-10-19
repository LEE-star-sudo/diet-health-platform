CREATE DATABASE IF NOT EXISTS lakehouse_db;

-- 用户个人数据。不含 password_hash。
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_db.dim_user_profile (
    user_id STRING,
    gender STRING,
    age INT,
    height_cm DECIMAL(5,2),
    weight_kg DECIMAL(5,2),
    waist_cm DECIMAL(5,2),
    bmi DECIMAL(4,2),
    target_calories DECIMAL(7,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    account STRING,
    display_name STRING,
    source_type STRING,
    nhanes_respondent_id BIGINT,
    activity_level STRING,
    pal_coefficient DECIMAL(4,3),
    goal_type STRING,
    target_weight_kg DECIMAL(5,2)
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.164.128:9000/lakehouse/dim/user_profile';

-- 食物字典，按食物编码关联饮食记录。不是用户行。
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_db.dim_food_nutrition_dict (
    id BIGINT,
    food_code STRING,
    food_name STRING,
    unit_weight_g DECIMAL(6,2),
    energy_kcal DECIMAL(6,2),
    carbohydrate_g DECIMAL(6,2),
    protein_g DECIMAL(6,2),
    total_fat_g DECIMAL(6,2),
    total_sugar_g DECIMAL(6,2),
    dietary_fiber_g DECIMAL(6,2),
    saturated_fat_g DECIMAL(6,2),
    monounsaturated_fat_g DECIMAL(6,2),
    polyunsaturated_fat_g DECIMAL(6,2),
    sodium_mg DECIMAL(7,2),
    potassium_mg DECIMAL(7,2),
    cholesterol_mg DECIMAL(7,2),
    vitamin_a_rae_mcg DECIMAL(7,2),
    vitamin_c_mg DECIMAL(7,2),
    vitamin_d_mcg DECIMAL(7,2),
    vitamin_e_mg DECIMAL(7,2),
    vitamin_k_mcg DECIMAL(7,2),
    thiamin_mg DECIMAL(7,2),
    riboflavin_mg DECIMAL(7,2),
    niacin_mg DECIMAL(7,2),
    vitamin_b6_mg DECIMAL(7,2),
    folate_mcg DECIMAL(7,2),
    vitamin_b12_mcg DECIMAL(7,2),
    calcium_mg DECIMAL(7,2),
    iron_mg DECIMAL(7,2),
    magnesium_mg DECIMAL(7,2),
    zinc_mg DECIMAL(7,2),
    selenium_mcg DECIMAL(7,2),
    caffeine_mg DECIMAL(7,2),
    alcohol_g DECIMAL(7,2),
    added_sugar_g DECIMAL(8,2),
    whole_grain_oz_eq DECIMAL(10,4),
    refined_grain_oz_eq DECIMAL(10,4),
    total_vegetable_cup_eq DECIMAL(10,4),
    dark_green_veg_beans_cup_eq DECIMAL(10,4),
    total_fruit_cup_eq DECIMAL(10,4),
    whole_fruit_cup_eq DECIMAL(10,4),
    dairy_cup_eq DECIMAL(10,4),
    seafood_plant_protein_oz_eq DECIMAL(10,4),
    fped_source STRING
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.164.128:9000/lakehouse/dim/food_nutrition_dict';

-- 手机打卡落到亚马逊 MySQL 后的完整行。
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_db.dwd_user_food_intake (
    id BIGINT,
    user_id STRING,
    record_time TIMESTAMP,
    meal_type STRING,
    food_code STRING,
    food_name STRING,
    portion_g DECIMAL(10,2),
    created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.164.128:9000/lakehouse/dwd/user_food_intake';

-- Spring Boot 换算后写入 MySQL 的完整营养行。
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_db.dwd_user_food_nutrition (
    id BIGINT,
    user_id STRING,
    intake_id BIGINT,
    event_time BIGINT,
    meal_type STRING,
    calories DOUBLE,
    sodium DOUBLE,
    potassium DOUBLE,
    carbohydrate_g DOUBLE,
    protein_g DOUBLE,
    total_fat_g DOUBLE,
    total_sugar_g DOUBLE,
    dietary_fiber_g DOUBLE,
    added_sugar_g DOUBLE,
    saturated_fat_g DOUBLE,
    monounsaturated_fat_g DOUBLE,
    polyunsaturated_fat_g DOUBLE,
    whole_grain_oz_eq DOUBLE,
    refined_grain_oz_eq DOUBLE,
    total_vegetable_cup_eq DOUBLE,
    dark_green_veg_beans_cup_eq DOUBLE,
    total_fruit_cup_eq DOUBLE,
    whole_fruit_cup_eq DOUBLE,
    dairy_cup_eq DOUBLE,
    seafood_plant_protein_oz_eq DOUBLE,
    pattern_data_available TINYINT,
    unsaturated_saturated_fat_ratio DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.164.128:9000/lakehouse/dwd/user_food_nutrition';

-- 亚马逊 Flink 写入 MySQL 的按人按天汇总。
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_db.dwd_user_daily_summary (
    id BIGINT,
    user_id STRING,
    stat_date DATE,
    total_calories DECIMAL(12,2),
    total_carbohydrate_g DECIMAL(12,2),
    total_protein_g DECIMAL(12,2),
    total_fat_g DECIMAL(12,2),
    total_sugar_g DECIMAL(12,2),
    total_dietary_fiber_g DECIMAL(12,2),
    total_added_sugar_g DECIMAL(12,2),
    total_saturated_fat_g DECIMAL(12,2),
    total_monounsaturated_fat_g DECIMAL(12,2),
    total_polyunsaturated_fat_g DECIMAL(12,2),
    total_sodium_mg DECIMAL(14,2),
    total_potassium_mg DECIMAL(14,2),
    total_whole_grain_oz_eq DECIMAL(14,4),
    total_refined_grain_oz_eq DECIMAL(14,4),
    total_vegetable_cup_eq DECIMAL(14,4),
    total_dark_green_veg_beans_cup_eq DECIMAL(14,4),
    total_fruit_cup_eq DECIMAL(14,4),
    total_whole_fruit_cup_eq DECIMAL(14,4),
    total_dairy_cup_eq DECIMAL(14,4),
    total_seafood_plant_protein_oz_eq DECIMAL(14,4),
    meal_count INT,
    unknown_pattern_count INT,
    is_calorie_surplus TINYINT,
    updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.164.128:9000/lakehouse/dwd/user_daily_summary';

-- 手机原始打卡。只含吃了什么，不含换算后的营养。
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_db.ods_meal_event (
    user_id STRING,
    event_time BIGINT,
    meal_type STRING,
    food_code STRING,
    food_name STRING,
    portion_g DOUBLE,
    stat_date STRING
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.164.128:9000/lakehouse/ods/meal_event';

-- 发件箱发出的换算结果。字段以 Kafka JSON 为准。
CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_db.ods_food_nutrition_event (
    nutrition_id BIGINT,
    intake_id BIGINT,
    user_id STRING,
    event_time BIGINT,
    meal_type STRING,
    food_code STRING,
    portion_g DOUBLE,
    calories DOUBLE,
    carbohydrate_g DOUBLE,
    protein_g DOUBLE,
    total_fat_g DOUBLE,
    total_sugar_g DOUBLE,
    dietary_fiber_g DOUBLE,
    saturated_fat_g DOUBLE,
    monounsaturated_fat_g DOUBLE,
    polyunsaturated_fat_g DOUBLE,
    sodium_mg DOUBLE,
    potassium_mg DOUBLE,
    pattern_data_available BOOLEAN,
    added_sugar_g DOUBLE,
    whole_grain_oz_eq DOUBLE,
    refined_grain_oz_eq DOUBLE,
    total_vegetable_cup_eq DOUBLE,
    dark_green_veg_beans_cup_eq DOUBLE,
    total_fruit_cup_eq DOUBLE,
    whole_fruit_cup_eq DOUBLE,
    dairy_cup_eq DOUBLE,
    seafood_plant_protein_oz_eq DOUBLE,
    stat_date STRING
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.164.128:9000/lakehouse/ods/food_nutrition_event';
