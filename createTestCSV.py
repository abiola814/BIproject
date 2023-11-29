import csv

def create_test_csv():
    data = [
        {"customer": "Customer1", "purchase": "ItemA", "budget": 100, "days": "monday"},
        {"customer": "Customer1", "purchase": "ItemB", "budget": 100, "days": "tuesday"},
        {"customer": "Customer2", "purchase": "ItemA", "budget": 150, "days": "wednesday"},
        {"customer": "Customer2", "purchase": "ItemC", "budget": 150, "days": "money"},
        {"customer": "Customer3", "purchase": "ItemB", "budget": 120, "days": 5},
        {"customer": "Customer3", "purchase": "ItemC", "budget": 120, "days": 6},
    ]

    with open("purchases_and_budget.csv", "w", newline="") as csvfile:
        fieldnames = ["customer", "purchase", "budget", "days"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    create_test_csv()
