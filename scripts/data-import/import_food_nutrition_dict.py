#!/usr/bin/env python3
"""Fill food_nutrition_dict from diet_foods_extracted.csv.

Only nutrients present in both the CSV and the table are written:
energy, carbohydrate, protein, total fat, and sodium. Values are per 100 g.
For each USDA food code, rows of at least 20 g are summed when any exist;
otherwise every row for that food is summed.

per 100 g = sum(nutrient) / sum(portion_g) * 100

Potassium is not in the CSV, so new rows keep the table default of 0.
"""

import csv
import os
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP

import pymysql

CSV_PATH = "/home/ubuntu/data/diet_foods_extracted.csv"
MIN_PORTION_G = 20
NAME_LIMIT = 128
NUTRIENT_LIMIT = Decimal("9999.99")
SODIUM_LIMIT = Decimal("99999.99")

NUTRIENTS = (
    "energy_kcal",
    "carbohydrate_g",
    "protein_g",
    "total_fat_g",
    "sodium_mg",
)


def per_100g(total, portion):
    value = (total / portion * Decimal(100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return value


def build_rows(path):
    grouped = defaultdict(list)
    with open(path, newline="", encoding="utf-8-sig") as handle:
        for record in csv.DictReader(handle):
            grouped[record["usda_food_code"]].append(record)

    rows = []
    large_portion_foods = 0
    small_only_foods = 0
    clamped = 0
    skipped_foods = 0
    for food_code, records in grouped.items():
        usable = [
            record for record in records
            if all(Decimal(record[nutrient]).is_finite() for nutrient in ("portion_g", *NUTRIENTS))
            and Decimal(record["portion_g"]) > 0
        ]
        if not usable:
            skipped_foods += 1
            continue
        large = [record for record in usable if Decimal(record["portion_g"]) >= MIN_PORTION_G]
        chosen = large or usable
        if large:
            large_portion_foods += 1
        else:
            small_only_foods += 1

        portion = sum(Decimal(record["portion_g"]) for record in chosen)
        totals = {
            nutrient: sum(Decimal(record[nutrient]) for record in chosen)
            for nutrient in NUTRIENTS
        }
        values = {nutrient: per_100g(totals[nutrient], portion) for nutrient in NUTRIENTS}
        for nutrient, value in values.items():
            limit = SODIUM_LIMIT if nutrient == "sodium_mg" else NUTRIENT_LIMIT
            if value > limit:
                values[nutrient] = limit
                clamped += 1
            elif value < 0:
                values[nutrient] = Decimal("0.00")
                clamped += 1

        food_name = Counter(record["food_name"] for record in records).most_common(1)[0][0]
        rows.append((
            food_code,
            food_name[:NAME_LIMIT],
            Decimal("100.00"),
            values["energy_kcal"],
            values["carbohydrate_g"],
            values["protein_g"],
            values["total_fat_g"],
            values["sodium_mg"],
        ))
    return rows, large_portion_foods, small_only_foods, clamped, skipped_foods


def main():
    rows, large_portion_foods, small_only_foods, clamped, skipped_foods = build_rows(CSV_PATH)
    connection = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password=os.environ["MYSQL_PASSWORD"],
        database="diet_health",
        charset="utf8mb4",
    )
    sql = """
        INSERT INTO food_nutrition_dict
            (food_code, food_name, unit_weight_g, energy_kcal, carbohydrate_g, protein_g, total_fat_g, sodium_mg)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            food_name = VALUES(food_name),
            unit_weight_g = VALUES(unit_weight_g),
            energy_kcal = VALUES(energy_kcal),
            carbohydrate_g = VALUES(carbohydrate_g),
            protein_g = VALUES(protein_g),
            total_fat_g = VALUES(total_fat_g),
            sodium_mg = VALUES(sodium_mg)
    """
    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        connection.commit()
    finally:
        connection.close()

    print(f"foods={len(rows)}")
    print(f"used_portions_at_least_{MIN_PORTION_G}g={large_portion_foods}")
    print(f"only_smaller_portions={small_only_foods}")
    print(f"clamped_values={clamped}")
    print(f"skipped_foods_without_numbers={skipped_foods}")


if __name__ == "__main__":
    main()
