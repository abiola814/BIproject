import csv
import matplotlib.pyplot as plt

# Dummy data (replace this with your actual data)
data = [
    {"customer_id": 1, "budget": 100, "price": 30, "age": 25, "is_member": True, "item_type": "fruit"},
    {"customer_id": 2, "budget": 150, "price": 20, "age": 30, "is_member": False, "item_type": "vegetable"},
    {"customer_id": 3, "budget": 200, "price": 25, "age": 22, "is_member": True, "item_type": "fruit"},
  
]

# Function to filter data based on user criteria
def filter_data(data, include_all=True, include_members=False, include_fruits=False):
    filtered_data = data.copy()

    if not include_all:
        if include_members:
            filtered_data = [entry for entry in filtered_data if entry["is_member"]]
        if include_fruits:
            filtered_data = [entry for entry in filtered_data if entry["item_type"] == "fruit"]

    return filtered_data

# Function to create scatter plots
def create_scatter_plot(x, y, title, x_label, y_label):
    plt.scatter(x, y)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.show()

# Filtered data based on user criteria
filtered_data = filter_data(data, include_all=True, include_members=True, include_fruits=False)

# Scatter plot: Customer budget vs price of items purchased
budgets = [entry["budget"] for entry in filtered_data]
prices = [entry["price"] for entry in filtered_data]
create_scatter_plot(budgets, prices, "Customer Budget vs Price", "Customer Budget", "Price of Items Purchased")

# Scatter plot: Customer age vs number of items purchased
ages = [entry["age"] for entry in filtered_data]
num_items = [1 for _ in filtered_data]  # Assuming each entry represents one item
create_scatter_plot(ages, num_items, "Customer Age vs Number of Items Purchased", "Customer Age", "Number of Items Purchased")
