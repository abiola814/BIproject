import csv
import sqlite3
import matplotlib.pyplot as plt

# Function to read CSV and create a dictionary
def csv_to_dict_single(csv_file):
    result_dict = {}
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = row['customer_name']
            if key in result_dict:
                result_dict[key].append(row)
            else:
                result_dict[key] = [row]
    return result_dict
def csv_to_dict(csv_file, key_column, value_column):
    result_dict = {}
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            key = row[key_column]
            value = row[value_column]
            if key in result_dict:
                result_dict[key].append(row)
            else:
                result_dict[key] = [row]
    return result_dict

# Function to query all foods purchased by a customer
def get_foods_by_customer(csv_file, customer_name):
    data_dict = csv_to_dict(csv_file, 'customer_name', 'food_name')
    return [entry['food_name'] for entry in data_dict.get(customer_name, [])]

# Function to query all days on which a customer shopped
def get_shopping_days_by_customer(csv_file, customer_name):
    data_dict = csv_to_dict(csv_file, 'customer_name', 'day')
    return [entry['day'] for entry in data_dict.get(customer_name, [])]

# Function to query all customers who have spent more than their budget
# Updated function to query all customers who have spent more than their budget
def get_customers_over_budget(csv_file):
    data_dict = csv_to_dict_single(csv_file)
    result_set = set()

    for customer_name, entries in data_dict.items():
        total_spent = sum(float(entry['price']) for entry in entries)
        budget = float(entries[0]['budget']) if entries and 'budget' in entries[0] else 0

        # Print debugging information
        print(f"Customer: {customer_name}, Total Spent: {total_spent}, Budget: {budget}")

        # Check if total spent is greater than the budget
        if total_spent > budget:
            result_set.add(customer_name)

    return result_set

# Function to create scatter plots
def create_scatter_plot(x, y, title, x_label, y_label):
    plt.scatter(x, y)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.show()

# Function to filter data based on user criteria
def filter_data(data, include_all=True, include_members=False, include_fruits=False):
    filtered_data = data.copy()

    if not include_all:
        if include_members:
            filtered_data = [entry for entry in filtered_data if entry.get("is_member")]
        if include_fruits:
            filtered_data = [entry for entry in filtered_data if entry.get("item_type") == "fruit"]

    return filtered_data

# Main part of the script
if __name__ == "__main__":
    # Get user input for CSV files
    purchases_csv = input("Enter the path to the purchases CSV file: ")

    # Load data from CSV
    with open(purchases_csv, 'r') as file:
        data = list(csv.DictReader(file))

    # Query all foods purchased by a customer
    customer_name_input = input("Enter the customer name to query for foods purchased: ")
    foods_purchased = get_foods_by_customer(purchases_csv, customer_name_input)
    print(f"Foods purchased by {customer_name_input}: {', '.join(foods_purchased)}")

    # Query all days on which a customer shopped
    shopping_days = get_shopping_days_by_customer(purchases_csv, customer_name_input)
    print(f"Days on which {customer_name_input} shopped: {', '.join(shopping_days)}")

    # Query all customers who have spent more than their budget
    customers_over_budget = get_customers_over_budget(purchases_csv)
    print(f"Customers who have spent more than their budget: {', '.join(customers_over_budget)}")

    # Create scatter plots with user-defined criteria
    include_all = input("Include all purchases? (y/n): ").lower() == 'y'
    include_members = input("Include only purchases made by members? (y/n): ").lower() == 'y'
    include_fruits = input("Include only purchases of fruits? (y/n): ").lower() == 'y'

    # Filtered data based on user criteria
    filtered_data = filter_data(data, include_all, include_members, include_fruits)

    # Scatter plot: Customer budget vs price of items purchased
    budgets = [float(entry["budget"]) for entry in filtered_data]
    prices = [float(entry["price"]) for entry in filtered_data]
    create_scatter_plot(budgets, prices, "Customer Budget vs Price", "Customer Budget", "Price of Items Purchased")

    # Scatter plot: Customer age vs number of items purchased
    ages = [float(entry["age"]) for entry in filtered_data]
    num_items = [1 for _ in filtered_data]  # Assuming each entry represents one item
    create_scatter_plot(ages, num_items, "Customer Age vs Number of Items Purchased", "Customer Age", "Number of Items Purchased")
