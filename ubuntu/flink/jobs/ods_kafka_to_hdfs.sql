SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.attached' = 'false';
SET 'parallelism.default' = '1';
SET 'table.local-time-zone' = 'Asia/Shanghai';

DROP TABLE IF EXISTS kafka_meal_event;
DROP TABLE IF EXISTS ods_meal_event;
DROP TABLE IF EXISTS kafka_food_nutrition;
DROP TABLE IF EXISTS ods_food_nutrition_event;

CREATE TABLE kafka_meal_event (
    userId STRING,
    `timestamp` BIGINT,
    mealType STRING,
    foodCode STRING,
    foodName STRING,
    portionG DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_meal_event_stream',
    'properties.bootstrap.servers' = 'KAFKA_BOOTSTRAP:9092',
    'properties.group.id' = 'lakehouse_ods_meal_event_v1',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ods_meal_event (
    user_id STRING,
    event_time BIGINT,
    meal_type STRING,
    food_code STRING,
    food_name STRING,
    portion_g DOUBLE,
    stat_date STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs://192.168.164.128:9000/lakehouse/ods/meal_event',
    'format' = 'parquet',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '10 min',
    'sink.rolling-policy.check-interval' = '1 min'
);

CREATE TABLE kafka_food_nutrition (
    nutritionId BIGINT,
    intakeId BIGINT,
    userId STRING,
    `timestamp` BIGINT,
    mealType STRING,
    foodCode STRING,
    portionG DOUBLE,
    calories DOUBLE,
    carbohydrateG DOUBLE,
    proteinG DOUBLE,
    totalFatG DOUBLE,
    totalSugarG DOUBLE,
    dietaryFiberG DOUBLE,
    saturatedFatG DOUBLE,
    monounsaturatedFatG DOUBLE,
    polyunsaturatedFatG DOUBLE,
    sodiumMg DOUBLE,
    potassiumMg DOUBLE,
    patternDataAvailable BOOLEAN,
    addedSugarG DOUBLE,
    wholeGrainOzEq DOUBLE,
    refinedGrainOzEq DOUBLE,
    totalVegetableCupEq DOUBLE,
    darkGreenVegBeansCupEq DOUBLE,
    totalFruitCupEq DOUBLE,
    wholeFruitCupEq DOUBLE,
    dairyCupEq DOUBLE,
    seafoodPlantProteinOzEq DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_food_nutrition_stream',
    'properties.bootstrap.servers' = 'KAFKA_BOOTSTRAP:9092',
    'properties.group.id' = 'lakehouse_ods_food_nutrition_v1',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ods_food_nutrition_event (
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
) WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs://192.168.164.128:9000/lakehouse/ods/food_nutrition_event',
    'format' = 'parquet',
    'sink.rolling-policy.file-size' = '128MB',
    'sink.rolling-policy.rollover-interval' = '10 min',
    'sink.rolling-policy.check-interval' = '1 min'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO ods_meal_event
SELECT
    userId,
    `timestamp`,
    mealType,
    foodCode,
    foodName,
    portionG,
    DATE_FORMAT(TO_TIMESTAMP_LTZ(`timestamp`, 3), 'yyyy-MM-dd')
FROM kafka_meal_event;

INSERT INTO ods_food_nutrition_event
SELECT
    nutritionId,
    intakeId,
    userId,
    `timestamp`,
    mealType,
    foodCode,
    portionG,
    calories,
    carbohydrateG,
    proteinG,
    totalFatG,
    totalSugarG,
    dietaryFiberG,
    saturatedFatG,
    monounsaturatedFatG,
    polyunsaturatedFatG,
    sodiumMg,
    potassiumMg,
    patternDataAvailable,
    addedSugarG,
    wholeGrainOzEq,
    refinedGrainOzEq,
    totalVegetableCupEq,
    darkGreenVegBeansCupEq,
    totalFruitCupEq,
    wholeFruitCupEq,
    dairyCupEq,
    seafoodPlantProteinOzEq,
    DATE_FORMAT(TO_TIMESTAMP_LTZ(`timestamp`, 3), 'yyyy-MM-dd')
FROM kafka_food_nutrition;
END;
