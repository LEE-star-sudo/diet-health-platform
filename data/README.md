[中文](#数据说明) | [English](#data-notes)

# 数据说明

这里的文件来自美国疾病控制与预防中心（CDC）国家健康统计中心（NCHS）公开发布的 National Health and Nutrition Examination Survey（NHANES，美国国家健康和营养检查调查）。周期是 2021 年 8 月至 2023 年 8 月，文件后缀为 `_L`。

这些数据只供学习、课程讨论和本项目的方法演示。它们不是医疗建议，不能用于诊断、治疗或临床决策。继续使用或在讨论中引用时，请保留来源说明。

官方入口：<https://wwwn.cdc.gov/nchs/nhanes/>

NHANES 公开数据由美国联邦政府制作，可按 NCHS 的公开数据使用方式用于学习和研究。本仓库不拥有这些调查数据的著作权。

## diet_foods_extracted.csv

从这一周期饮食调查第一日的个人食物记录整理而来，共 100116 行、6751 名受访者。一行是一个人在某个钟点吃的一种食物。

| 字段 | 含义 |
|---|---|
| `respondent_id` | 受访者编号，即 NHANES 的 `SEQN`，范围 130378–142310 |
| `age`、`gender`、`race_ethnicity` | 年龄、性别、种族 |
| `bmi`、`weight_kg`、`height_cm`、`waist_cm` | 体检得到的体质指数、体重、身高、腰围 |
| `eating_time`、`meal_occasion_code`、`meal_type` | 进食钟点、餐次编码、餐次 |
| `usda_food_code`、`food_name`、`portion_g` | USDA 食物编码、食物名、食用克数 |
| `energy_kcal`、`carbohydrate_g`、`protein_g`、`total_fat_g`、`total_sugar_g`、`sodium_mg` | 这一份食物的热量、碳水、蛋白质、总脂肪、总糖、钠 |

这份 CSV 只有钟点，没有日历日期。同一个人的年龄和体检指标在各行相同。它是整理后的学习样本，不是 CDC 的原始 XPT。实时打卡链路不直接读这个文件。档案导入和食物字典里的热量、碳水、蛋白质、总脂肪、钠来自这份表。

## nhanes/2021-2023/

同一周期的原始 XPT，从 CDC 按文件下载。地址形式为：

`https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/<文件名>`

网站上有的页面写成 2021–2022，后缀 `_L` 的文件对应该公开周期，受访者编号与上面的 CSV 一致。

| 文件 | 内容 |
|---|---|
| `PAQ_L.xpt`、`PAQY_L.xpt` | 成人和青少年体力活动 |
| `HDL_L.xpt`、`TRIGLY_L.xpt`、`TCHOL_L.xpt` | 高密度脂蛋白、甘油三酯、总胆固醇 |
| `GLU_L.xpt`、`GHB_L.xpt` | 空腹血糖、糖化血红蛋白 |
| `BPXO_L.xpt`、`BPQ_L.xpt` | 血压测量，以及是否在使用降压药、降脂药 |
| `BIOPRO_L.xpt` | 肝功能等生化指标 |
| `HSCRP_L.xpt` | 高敏 C 反应蛋白。这是血液炎症指标，不是膳食炎症指数的原料 |
| `SLQ_L.xpt`、`SMQ_L.xpt`、`ALQ_L.xpt` | 睡眠、吸烟、饮酒 |
| `DIQ_L.xpt`、`MCQ_L.xpt` | 糖尿病和自报病史 |
| `WHQ_L.xpt` | 自报的既往体重，不是连续多天的实测体重 |

FNDDS、FPED、FPID 的电子表格在亚马逊服务器上，用来补食物字典，这次没有放进仓库。

---

[中文](#数据说明) | [English](#data-notes)

# Data notes

These files come from the public National Health and Nutrition Examination Survey (NHANES), released by the National Center for Health Statistics (NCHS) at the U.S. Centers for Disease Control and Prevention (CDC). The cycle is August 2021 through August 2023. Official file names use the suffix `_L`.

The files are included for learning, classroom discussion, and method demonstrations in this project. They are not medical advice and must not be used for diagnosis, treatment, or clinical decisions. Keep this source note when you reuse or cite the files.

Official entry point: <https://wwwn.cdc.gov/nchs/nhanes/>

NHANES public-use data are produced by the U.S. federal government and may be used for learning and research under NCHS public-use terms. This repository does not claim copyright in the survey data.

## diet_foods_extracted.csv

This table was prepared from day-1 individual food records in that cycle. It has 100,116 rows and 6,751 respondents. One row is one food eaten by one person at one clock time.

| Column | Meaning |
|---|---|
| `respondent_id` | NHANES respondent sequence number, `SEQN`, from 130378 to 142310 |
| `age`, `gender`, `race_ethnicity` | Age, sex, and race or ethnicity |
| `bmi`, `weight_kg`, `height_cm`, `waist_cm` | Examined BMI, weight, height, and waist circumference |
| `eating_time`, `meal_occasion_code`, `meal_type` | Clock time, meal-occasion code, and meal name |
| `usda_food_code`, `food_name`, `portion_g` | USDA food code, food name, and grams eaten |
| `energy_kcal`, `carbohydrate_g`, `protein_g`, `total_fat_g`, `total_sugar_g`, `sodium_mg` | Energy, carbohydrate, protein, total fat, total sugar, and sodium for that portion |

The CSV has a clock time and no calendar date. Age and body measures stay the same across a person's rows. It is a learning extract, not a CDC XPT. The realtime check-in path does not read this file. Profile import, and the calorie, carbohydrate, protein, total fat, and sodium values in the food dictionary, come from it.

## nhanes/2021-2023/

Original XPT files for the same cycle, downloaded individually from the CDC. The URL pattern is:

`https://wwwn.cdc.gov/Nchs/Data/Nhanes/Public/2021/DataFiles/<file name>`

Some CDC pages title the cycle 2021–2022. Files with the suffix `_L` are the public release for this cycle, and their respondent IDs match the CSV above.

| File | Contents |
|---|---|
| `PAQ_L.xpt`, `PAQY_L.xpt` | Physical activity for adults and for youth |
| `HDL_L.xpt`, `TRIGLY_L.xpt`, `TCHOL_L.xpt` | HDL cholesterol, triglycerides, and total cholesterol |
| `GLU_L.xpt`, `GHB_L.xpt` | Fasting glucose and glycated hemoglobin |
| `BPXO_L.xpt`, `BPQ_L.xpt` | Blood pressure measurements, and whether blood-pressure or cholesterol medication was reported |
| `BIOPRO_L.xpt` | Biochemistry panel, including liver measures |
| `HSCRP_L.xpt` | High-sensitivity C-reactive protein. This is a blood inflammation marker, not an input of the dietary inflammatory index |
| `SLQ_L.xpt`, `SMQ_L.xpt`, `ALQ_L.xpt` | Sleep, smoking, and alcohol |
| `DIQ_L.xpt`, `MCQ_L.xpt` | Diabetes and self-reported medical conditions |
| `WHQ_L.xpt` | Self-reported weight history, not a series of measured weights across days |

The FNDDS, FPED, and FPID spreadsheets used to complete the food dictionary stay on the Amazon server and are not in this commit.
