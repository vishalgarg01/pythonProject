import csv

header = [
    "uniqueId",
    "billAmount","source","billingDate","type","billNumber",
    "identifierType","identifierValue","First Name","Last Name",
    "Email","Amount","description","qty","rate","value"
]

row = [
    "17","INSTORE","2026-01-13 18:31:19","REGULAR","3561880438",
    "MOBILE","918959763596","autofn_918959763596","autoln_918959763596",
    "918959763596@gmail.com","2840","AUTOMATION","20","74","27"
]

with open("billing_1_million_records.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)

    for i in range(1, 100001):
        writer.writerow([f"unique_{i}"] + row)
