#!/usr/bin/env python3
"""Fill missing per-100g nutrients in food_nutrition_dict from FNDDS 2021-2023.

The workbook is already per 100 g, matching unit_weight_g. Existing energy,
carbohydrate, protein, total fat, and sodium values calculated from the CSV
are left unchanged. Potassium and the newly added columns are filled only
for food codes present in both tables.
"""

import os
from decimal import Decimal, ROUND_HALF_UP

import openpyxl
import pymysql

WORKBOOK = "/home/ubuntu/data/FNDDS_2021-2023_Nutrient_Values.xlsx"
COLUMNS = {
    "Potassium (mg)": "potassium_mg",
    "Sugars, total (g)": "total_sugar_g",
    "Fiber, total dietary (g)": "dietary_fiber_g",
    "Fatty acids, total saturated (g)": "saturated_fat_g",
    "Fatty acids, total monounsaturated (g)": "monounsaturated_fat_g",
    "Fatty acids, total polyunsaturated (g)": "polyunsaturated_fat_g",
    "Cholesterol (mg)": "cholesterol_mg",
    "Vitamin A, RAE (mcg_RAE)": "vitamin_a_rae_mcg",
    "Vitamin C (mg)": "vitamin_c_mg",
    "Vitamin D (D2 + D3) (mcg)": "vitamin_d_mcg",
    "Vitamin E (alpha-tocopherol) (mg)": "vitamin_e_mg",
    "Vitamin K (phylloquinone) (mcg)": "vitamin_k_mcg",
    "Thiamin (mg)": "thiamin_mg",
    "Riboflavin (mg)": "riboflavin_mg",
    "Niacin (mg)": "niacin_mg",
    "Vitamin B-6 (mg)": "vitamin_b6_mg",
    "Folate, total (mcg)": "folate_mcg",
    "Vitamin B-12 (mcg)": "vitamin_b12_mcg",
    "Calcium (mg)": "calcium_mg",
    "Iron (mg)": "iron_mg",
    "Magnesium (mg)": "magnesium_mg",
    "Zinc (mg)": "zinc_mg",
    "Selenium (mcg)": "selenium_mcg",
    "Caffeine (mg)": "caffeine_mg",
    "Alcohol (g)": "alcohol_g",
}
LIMITS = {
    column: Decimal("9999.99")
    if column in {"total_sugar_g", "dietary_fiber_g", "saturated_fat_g", "monounsaturated_fat_g", "polyunsaturated_fat_g"}
    else Decimal("99999.99")
    for column in COLUMNS.values()
}


def normalize_header(name):
    return " ".join(str(name).split())


def as_decimal(value):
    if value is None or value == "":
        return Decimal("0.00")
    number = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if not number.is_finite() or number < 0:
        return Decimal("0.00")
    return number


def load_fndds(path):
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet = workbook["FNDDS Nutrient Values"]
    rows = sheet.iter_rows(values_only=True)
    next(rows)
    header = [normalize_header(name) if name else "" for name in next(rows)]
    indexes = {name: header.index(name) for name in COLUMNS}
    foods = {}
    for row in rows:
        if row[0] is None:
            continue
        food_code = str(int(row[0])) if isinstance(row[0], float) else str(row[0]).strip()
        values = {}
        for source, target in COLUMNS.items():
            number = as_decimal(row[indexes[source]])
            if number > LIMITS[target]:
                number = LIMITS[target]
            values[target] = number
        foods[food_code] = values
    workbook.close()
    return foods


def main():
    foods = load_fndds(WORKBOOK)
    connection = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password=os.environ["MYSQL_PASSWORD"],
        database="diet_health",
        charset="utf8mb4",
    )
    assignments = ", ".join(f"{column} = %s" for column in COLUMNS.values())
    sql = f"""
        UPDATE food_nutrition_dict
        SET {assignments}
        WHERE food_code = %s
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT food_code FROM food_nutrition_dict")
            existing = {row[0] for row in cursor.fetchall()}
            matched = existing & foods.keys()
            payload = [
                tuple(foods[code][column] for column in COLUMNS.values()) + (code,)
                for code in matched
            ]
            cursor.executemany(sql, payload)
        connection.commit()
    finally:
        connection.close()
    print(f"fndds_foods={len(foods)}")
    print(f"dictionary_foods={len(existing)}")
    print(f"updated={len(matched)}")
    print(f"dictionary_without_fndds={len(existing - foods.keys())}")


if __name__ == "__main__":
    main()
