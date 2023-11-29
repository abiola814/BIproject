import csv

def csv_to_dict(file, key_col, value_col):
  # file: a CSV file open for reading
  # key_col: a string representing the column to be used as keys
  # value_col: a string representing the column to be used as values
  # returns: a dictionary mapping keys to sets of values

  # create an empty dictionary
  result = {}

  # create a csv reader object
  reader = csv.DictReader(file)

  # loop through each row in the file
  for row in reader:
    # get the key and value from the row
    key = row[key_col]
    value = row[value_col]

    # if the key is not in the dictionary, create a new set for it
    if key not in result:
      result[key] = set()

    # add the value to the set of the key
    result[key].add(value)

  # return the dictionary
  return result


import csv
import pandas as pd

def query_csv(file_name, query):
  # file_name: a string representing the name of a csv file
  # query: a string representing the query to be performed
  # prints the output of the query

  # read the csv file as a pandas dataframe
  df = pd.read_csv(file_name)

  # check the query type and perform the corresponding operation
  if query.startswith("all foods purchased by "):
    # get the customer name from the query
    customer_name = query[len("all foods purchased by "):]

    # filter the dataframe by the customer name
    df_customer = df[df["customer name"] == customer_name]

    # check if the customer name is valid
    if not df_customer.empty:
      # get the unique foods purchased by the customer
      foods = df_customer["food"].unique()

      # print the set of foods purchased by the customer
      print(f"{customer_name} purchased {set(foods)}")
    else:
      # print an error message
      print(f"{customer_name} is not a valid customer name")

  elif query.startswith("all days on which "):
    # get the customer name from the query
    customer_name = query[len("all days on which "):]

    # filter the dataframe by the customer name
    df_customer = df[df["customer name"] == customer_name]

    # check if the customer name is valid
    if not df_customer.empty:
      # get the unique days on which the customer shopped
      days = df_customer["day"].unique()

      # print the set of days on which the customer shopped
      print(f"{customer_name} shopped on {set(days)}")
    else:
      # print an error message
      print(f"{customer_name} is not a valid customer name")

  elif query == "all customers who have spent more than their budget":
    # group the dataframe by the customer name and sum the spending
    df_spending = df.groupby("customer name")["spending"].sum()

    # merge the spending dataframe with the budget dataframe
    df_merged = df_spending.to_frame().merge(df[["customer name", "budget"]].drop_duplicates(), on="customer name")

    # convert the budget column to numeric
    df_merged["budget"] = pd.to_numeric(df_merged["budget"])

    # filter the dataframe by the condition that spending is more than budget
    df_overspenders = df_merged[df_merged["spending"] > df_merged["budget"]]

    # get the list of overspenders
    overspenders = df_overspenders["customer name"].tolist()

    # print the list of overspenders
    print(f"The customers who have spent more than their budget are {overspenders}")

  else:
    # print an error message
    print(f"{query} is not a valid query")

query_csv("purchases_and_budget.csv", "all foods purchased by Alice")
query_csv("purchases_and_budget.csv", "all days on which Bob")
query_csv("purchases_and_budget.csv", "all customers who have spent more than their budget")
query_csv("purchases_and_budget.csv", "all foods purchased by David")
query_csv("purchases_and_budget.csv", "all customers who have spent less than their budget")