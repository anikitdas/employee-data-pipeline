import random
import csv
from datetime import datetime, timedelta

first_names = ["John","Jane","Alice","Bob","Tom","Emma","Liam","Noah","Olivia","Ava"]
last_names = ["Smith","Doe","Brown","Johnson","Lee","Wilson","Taylor","Anderson"]
domains = ["company.com","email.com","test.com"]

def random_date(start_year=2015, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end-start).days))

with open("data/employees_raw.csv", "w", newline="") as f:
    writer = csv.writer(f)

    writer.writerow([
        "employee_id","first_name","last_name","email","hire_date",
        "job_title","department","salary","manager_id","address",
        "city","state","zip_code","birth_date","status"
    ])

    for i in range(1000):
        first = random.choice(first_names)
        last = random.choice(last_names)

        email = f"{first.lower()}.{last.lower()}{i}@{random.choice(domains)}"

        writer.writerow([
            1000 + i,
            first,
            last,
            email,
            random_date().date(),
            random.choice(["Engineer","Analyst","Manager","HR"]),
            random.choice(["IT","HR","Finance","Analytics"]),
            f"${random.randint(30000,100000):,}",
            random.randint(2000,3000),
            "123 Street",
            "City",
            "ST",
            "12345",
            random_date(1965, 2000).date(),
            "Active"
        ])

print("✅ 1000 records generated (NO Faker needed)")