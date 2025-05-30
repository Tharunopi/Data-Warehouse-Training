import pandas as pd

df = pd.read_csv("C:\Stack overflow\Data-Warehouse-Training\May-30\Code\orders.csv")

# print(df.head())

# delivered = df.query("Status == 'Delivered'")
# print(delivered)

# print(df['Status'].value_counts())

# df['Month'] = pd.to_datetime(df['OrderDate']).dt.month

# grouped = df.groupby(["Month", "Status"]).size().reset_index(name="Count")
# print(grouped)

# subset = df[["OrderID", "CustomerName", "Status"]]
# print(subset.head())

# customer_info = {
#     1:"Hyderabad",
#     2:"Mumbai",
#     3:"Delhi",
#     4:"BLR"
# }

# df["City"] = df["CustomerID"].map(customer_info)

# print(df)

# print(df.groupby(by="CustomerName").agg(
#     {
#         "TotalAmount": ["sum", "mean"],
#         "OrderID": "count"
#     }
# ))

# print(df.sort_values(by="TotalAmount", ascending=False))

# df.to_json("filtered_json", index=False)

