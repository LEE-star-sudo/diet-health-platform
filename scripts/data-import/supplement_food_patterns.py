#!/usr/bin/env python3
"""Fill HEI food-pattern columns on food_nutrition_dict.

Direct matches come from FPED 2017-March 2020, which is per 100 g.
Foods absent from that file are rebuilt from FNDDS 2021-2023 recipes:
each ingredient is looked up in FPID 2017-2018, or in FPED if the
ingredient is itself an older food code. Equivalents are scaled to 100 g
of the finished food, using the recipe's moisture change.

Added sugar is stored in grams: teaspoon equivalents x 4.2.
Vegetables include legumes, because FPED's V_TOTAL does not.
Foods that still cannot be completed stay NULL and are marked missing.
"""

import os
from decimal import Decimal, ROUND_HALF_UP

import openpyxl
import pymysql
import xlrd

FPED_PATH = "/home/ubuntu/data/FPED_1720.xls"
FPID_PATH = "/home/ubuntu/data/FPID_1718.xls"
INGREDIENT_PATH = "/home/ubuntu/data/FNDDS_2021-2023_Ingredients.xlsx"
ADDED_SUGAR_GRAMS_PER_TEASPOON = Decimal("4.2")
RAW_KEYS = (
    "F_TOTAL",
    "F_JUICE",
    "V_TOTAL",
    "V_DRKGR",
    "V_LEGUMES",
    "G_WHOLE",
    "G_REFINED",
    "PF_SEAFD_HI",
    "PF_SEAFD_LOW",
    "PF_SOY",
    "PF_NUTSDS",
    "PF_LEGUMES",
    "D_TOTAL",
    "ADD_SUGARS",
)
OUTPUT_COLUMNS = (
    "added_sugar_g",
    "whole_grain_oz_eq",
    "refined_grain_oz_eq",
    "total_vegetable_cup_eq",
    "dark_green_veg_beans_cup_eq",
    "total_fruit_cup_eq",
    "whole_fruit_cup_eq",
    "dairy_cup_eq",
    "seafood_plant_protein_oz_eq",
    "fped_source",
)


def code_of(value):
    if value is None or value == "":
        return None
    if isinstance(value, float):
        return str(int(value))
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text


def decimal_of(value):
    if value is None or value == "":
        return Decimal("0")
    return Decimal(str(value))


def quantize(value, places):
    return value.quantize(Decimal(places), rounding=ROUND_HALF_UP)


def header_index(headers):
    found = {}
    for index, name in enumerate(headers):
        token = str(name).split()[0].rstrip(".")
        found[token] = index
    missing = [key for key in RAW_KEYS if key not in found]
    if missing:
        raise SystemExit(f"missing columns: {missing}")
    return found


def load_pattern_sheet(path, code_column):
    sheet = xlrd.open_workbook(path).sheet_by_index(0)
    headers = sheet.row_values(0)
    indexes = header_index(headers)
    code_index = headers.index(code_column)
    patterns = {}
    for row_number in range(1, sheet.nrows):
        food_code = code_of(sheet.cell_value(row_number, code_index))
        if not food_code:
            continue
        patterns[food_code] = {
            key: decimal_of(sheet.cell_value(row_number, indexes[key]))
            for key in RAW_KEYS
        }
    return patterns


def load_recipes(path):
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet = workbook["FNDDS Ingredients"]
    rows = sheet.iter_rows(values_only=True)
    next(rows)
    next(rows)
    recipes = {}
    for row in rows:
        food_code = code_of(row[0])
        ingredient_code = code_of(row[5])
        if not food_code or not ingredient_code:
            continue
        recipe = recipes.setdefault(food_code, {"ingredients": [], "moisture": decimal_of(row[9])})
        recipe["ingredients"].append((ingredient_code, decimal_of(row[7])))
    workbook.close()
    return recipes


def derive(raw):
    whole_fruit = raw["F_TOTAL"] - raw["F_JUICE"]
    if whole_fruit < 0:
        whole_fruit = Decimal("0")
    seafood_plant = (
        raw["PF_SEAFD_HI"] + raw["PF_SEAFD_LOW"] + raw["PF_SOY"] + raw["PF_NUTSDS"] + raw["PF_LEGUMES"]
    )
    return {
        "added_sugar_g": quantize(raw["ADD_SUGARS"] * ADDED_SUGAR_GRAMS_PER_TEASPOON, "0.01"),
        "whole_grain_oz_eq": quantize(raw["G_WHOLE"], "0.0001"),
        "refined_grain_oz_eq": quantize(raw["G_REFINED"], "0.0001"),
        "total_vegetable_cup_eq": quantize(raw["V_TOTAL"] + raw["V_LEGUMES"], "0.0001"),
        "dark_green_veg_beans_cup_eq": quantize(raw["V_DRKGR"] + raw["V_LEGUMES"], "0.0001"),
        "total_fruit_cup_eq": quantize(raw["F_TOTAL"], "0.0001"),
        "whole_fruit_cup_eq": quantize(whole_fruit, "0.0001"),
        "dairy_cup_eq": quantize(raw["D_TOTAL"], "0.0001"),
        "seafood_plant_protein_oz_eq": quantize(seafood_plant, "0.0001"),
    }


def rollup(recipe, fpid, fped):
    total_weight = sum(weight for _, weight in recipe["ingredients"])
    finished_weight = total_weight * (Decimal("1") + recipe["moisture"] / Decimal("100"))
    if finished_weight <= 0:
        return None
    totals = {key: Decimal("0") for key in RAW_KEYS}
    for ingredient_code, weight in recipe["ingredients"]:
        pattern = fpid.get(ingredient_code) or fped.get(ingredient_code)
        if pattern is None:
            return None
        for key in RAW_KEYS:
            totals[key] += pattern[key] * weight / Decimal("100")
    scale = Decimal("100") / finished_weight
    return {key: value * scale for key, value in totals.items()}


def main():
    fped = load_pattern_sheet(FPED_PATH, "FOODCODE")
    fpid = load_pattern_sheet(FPID_PATH, "CODE")
    recipes = load_recipes(INGREDIENT_PATH)
    connection = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password=os.environ["MYSQL_PASSWORD"],
        database="diet_health",
        charset="utf8mb4",
    )
    assignments = ", ".join(f"{column} = %s" for column in OUTPUT_COLUMNS)
    sql = f"UPDATE food_nutrition_dict SET {assignments} WHERE food_code = %s"
    counts = {"fped_1720": 0, "ingredient_rollup": 0, "missing": 0}
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT food_code FROM food_nutrition_dict")
            food_codes = [row[0] for row in cursor.fetchall()]
            payload = []
            for food_code in food_codes:
                raw = fped.get(food_code)
                source = "fped_1720"
                if raw is None and food_code in recipes:
                    raw = rollup(recipes[food_code], fpid, fped)
                    source = "ingredient_rollup" if raw is not None else "missing"
                elif raw is None:
                    source = "missing"
                counts[source] += 1
                if raw is None:
                    values = {column: None for column in OUTPUT_COLUMNS if column != "fped_source"}
                else:
                    values = derive(raw)
                payload.append(tuple(values[column] if column != "fped_source" else source for column in OUTPUT_COLUMNS) + (food_code,))
            cursor.executemany(sql, payload)
        connection.commit()
    finally:
        connection.close()
    print(f"dictionary_foods={len(food_codes)}")
    for source, count in counts.items():
        print(f"{source}={count}")


if __name__ == "__main__":
    main()
