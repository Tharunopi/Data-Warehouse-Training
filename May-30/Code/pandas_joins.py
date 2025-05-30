import pandas as pd

orders = pd.read_csv(r"C:\Stack overflow\Data-Warehouse-Training\May-30\Code\order.csv")
customers = pd.read_csv(r"C:\Stack overflow\Data-Warehouse-Training\May-30\Code\customer.csv")
products = pd.read_csv(r"C:\Stack overflow\Data-Warehouse-Training\May-30\Code\product.csv")

# result = pd.merge(orders, customers, on="CustomerID", how="inner")
# print(result.head())

# left = pd.merge(orders, customers, on="CustomerID", how="left")
# print(left)

# right = pd.merge(orders, customers, on="CustomerID", how="right")
# print(right)

full = pd.merge(orders, customers, on="CustomerID", how="outer")
print(full)