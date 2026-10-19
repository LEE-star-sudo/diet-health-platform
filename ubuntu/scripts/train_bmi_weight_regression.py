"""用单日饮食汇总预测成人体检 BMI 和体重。

每人在 CSV 里只有一天的多条食物记录，没有连续日期，所以这里做的是
截面回归，不是未来体重轨迹。预测 BMI 时不用体重、腰围和 BMI 本身；
预测体重时不用 BMI、腰围和体重本身。
"""

import json

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CSV_PATH = "/home/ccl/data/diet_foods_extracted (1).csv"
MODEL_DIR = "/home/ccl/diet-health/models"
NUTRIENT_COLS = [
    "energy_kcal",
    "carbohydrate_g",
    "protein_g",
    "total_fat_g",
    "total_sugar_g",
    "sodium_mg",
]


def metrics(predictions, label):
    out = {}
    for name in ("rmse", "mae", "r2"):
        evaluator = RegressionEvaluator(labelCol=label, predictionCol="prediction", metricName=name)
        out[name] = evaluator.evaluate(predictions)
    return out


def build_frame(spark):
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(CSV_PATH)
    )
    nutrient_missing = F.lit(False)
    for column in NUTRIENT_COLS + ["portion_g"]:
        nutrient_missing = nutrient_missing | F.col(column).isNull()

    person = raw.groupBy("respondent_id").agg(
        F.first("age", ignorenulls=True).alias("age"),
        F.first("gender", ignorenulls=True).alias("gender"),
        F.first("race_ethnicity", ignorenulls=True).alias("race_ethnicity"),
        F.first("bmi", ignorenulls=True).alias("bmi"),
        F.first("weight_kg", ignorenulls=True).alias("weight_kg"),
        F.first("height_cm", ignorenulls=True).alias("height_cm"),
        F.first("waist_cm", ignorenulls=True).alias("waist_cm"),
        F.sum("energy_kcal").alias("daily_energy_kcal"),
        F.sum("carbohydrate_g").alias("daily_carbohydrate_g"),
        F.sum("protein_g").alias("daily_protein_g"),
        F.sum("total_fat_g").alias("daily_total_fat_g"),
        F.sum("total_sugar_g").alias("daily_total_sugar_g"),
        F.sum("sodium_mg").alias("daily_sodium_mg"),
        F.count("*").alias("food_rows"),
        F.sum(F.when(nutrient_missing, 1).otherwise(0)).alias("incomplete_food_rows"),
    )
    return person


def fit_target(person, label, numeric_features):
    data = person.filter(
        (F.col("age") >= 18)
        & F.col(label).isNotNull()
        & F.col("height_cm").isNotNull()
        & (F.col("incomplete_food_rows") == 0)
        & F.col("gender").isNotNull()
        & F.col("race_ethnicity").isNotNull()
    )
    count = data.count()
    train, test = data.randomSplit([0.8, 0.2], seed=42)
    feature_columns = numeric_features + ["gender_vec", "race_vec"]
    indexer_gender = StringIndexer(inputCol="gender", outputCol="gender_idx", handleInvalid="keep")
    indexer_race = StringIndexer(inputCol="race_ethnicity", outputCol="race_idx", handleInvalid="keep")
    encoder = OneHotEncoder(
        inputCols=["gender_idx", "race_idx"],
        outputCols=["gender_vec", "race_vec"],
        handleInvalid="keep",
    )
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features_raw", handleInvalid="skip")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)

    linear = Pipeline(stages=[
        indexer_gender, indexer_race, encoder, assembler, scaler,
        LinearRegression(featuresCol="features", labelCol=label, maxIter=50),
    ])
    forest = Pipeline(stages=[
        indexer_gender, indexer_race, encoder, assembler,
        RandomForestRegressor(
            featuresCol="features_raw",
            labelCol=label,
            numTrees=100,
            maxDepth=8,
            seed=42,
        ),
    ])

    linear_model = linear.fit(train)
    forest_model = forest.fit(train)
    linear_pred = linear_model.transform(test)
    forest_pred = forest_model.transform(test)
    mean_value = train.agg(F.avg(label)).first()[0]
    baseline = test.withColumn("prediction", F.lit(mean_value))

    importances = list(
        zip(
            feature_columns,
            forest_model.stages[-1].featureImportances.toArray().tolist(),
        )
    )
    # 随机森林的重要性是在独热编码之后的向量上，长度和原始列名不一定一一对应。
    # 这里只保留数值列在向量前部的重要性，分类变量另看整体拟合。
    numeric_importance = {
        name: float(value)
        for name, value in zip(numeric_features, forest_model.stages[-1].featureImportances.toArray().tolist())
    }

    return {
        "label": label,
        "adults_used": count,
        "train_rows": train.count(),
        "test_rows": test.count(),
        "excluded_features": ["weight_kg", "bmi", "waist_cm"] if label == "bmi" else ["bmi", "weight_kg", "waist_cm"],
        "numeric_features": numeric_features,
        "baseline_mean": metrics(baseline, label),
        "linear_regression": metrics(linear_pred, label),
        "random_forest_regression": metrics(forest_pred, label),
        "random_forest_numeric_importance": numeric_importance,
    }, linear_model, forest_model


def main():
    spark = (
        SparkSession.builder.appName("bmi-weight-regression")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    person = build_frame(spark).cache()
    total_people = person.count()
    adults = person.filter(F.col("age") >= 18).count()

    diet_features = [
        "age",
        "height_cm",
        "daily_energy_kcal",
        "daily_carbohydrate_g",
        "daily_protein_g",
        "daily_total_fat_g",
        "daily_total_sugar_g",
        "daily_sodium_mg",
        "food_rows",
    ]
    bmi_report, bmi_linear, bmi_forest = fit_target(person, "bmi", diet_features)
    weight_report, weight_linear, weight_forest = fit_target(person, "weight_kg", diet_features)

    report = {
        "source": CSV_PATH,
        "people": total_people,
        "adults_age_ge_18": adults,
        "note": "每人只有一天的食物记录。模型预测的是当天体检得到的 BMI 或体重，不是未来体重变化。",
        "bmi": bmi_report,
        "weight_kg": weight_report,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
