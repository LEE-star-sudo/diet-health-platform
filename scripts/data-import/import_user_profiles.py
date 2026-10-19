#!/usr/bin/env python3
"""Create one user_profile row for each CSV respondent.

user_id equals the respondent id, so later meal rows can use the same id.
Activity, goal, and target calories stay empty because the survey does not
contain them. People younger than 18 are included; the phone registration
API rejects those ages.
"""

import csv
import hashlib
import math
import os
from decimal import Decimal, ROUND_HALF_UP

import pymysql

CSV_PATH = "/home/ubuntu/data/diet_foods_extracted.csv"
PASSWORD = os.environ["SURVEY_USER_PASSWORD"]


def number(value, places):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return Decimal(str(parsed)).quantize(Decimal(places), rounding=ROUND_HALF_UP)


def password_hash():
    salt = os.urandom(16).hex()
    digest = hashlib.pbkdf2_hmac("sha256", PASSWORD.encode(), salt.encode(), 120_000, dklen=32)
    return f"pbkdf2_sha256$120000${salt}${digest.hex()}"


def respondents(path):
    people = {}
    with open(path, newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            people.setdefault(row["respondent_id"].strip(), row)
    return people


def main():
    shared_hash = password_hash()
    rows = []
    for respondent_id, row in respondents(CSV_PATH).items():
        gender = row["gender"].strip()
        if gender not in {"Male", "Female"}:
            raise SystemExit(f"unrecognized gender for {respondent_id}: {gender}")
        rows.append((
            respondent_id,
            respondent_id,
            shared_hash,
            f"用户{respondent_id}",
            "nhanes",
            int(respondent_id),
            int(float(row["age"])),
            gender,
            number(row["height_cm"], "0.01"),
            number(row["weight_kg"], "0.01"),
            number(row["waist_cm"], "0.01"),
            number(row["bmi"], "0.01"),
        ))

    connection = pymysql.connect(
        host="127.0.0.1",
        user="root",
        password=os.environ["MYSQL_PASSWORD"],
        database="diet_health",
        charset="utf8mb4",
    )
    sql = """
        INSERT INTO user_profile (
            user_id, account, password_hash, display_name, source_type,
            nhanes_respondent_id, age, gender, height_cm, weight_kg, waist_cm, bmi,
            activity_level, pal_coefficient, goal_type, target_weight_kg, target_calories
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL)
        ON DUPLICATE KEY UPDATE
            nhanes_respondent_id = VALUES(nhanes_respondent_id),
            age = VALUES(age),
            gender = VALUES(gender),
            height_cm = VALUES(height_cm),
            weight_kg = VALUES(weight_kg),
            waist_cm = VALUES(waist_cm),
            bmi = VALUES(bmi)
    """
    try:
        with connection.cursor() as cursor:
            cursor.executemany(sql, rows)
        connection.commit()
    finally:
        connection.close()
    print(f"respondents={len(rows)}")


if __name__ == "__main__":
    main()
