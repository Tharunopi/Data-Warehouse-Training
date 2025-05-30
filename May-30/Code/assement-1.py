# for i in range(11, 50, 2):
#     print(f"Odd Numbers: {i}")

# def leepYear(x):
#     if (x % 4 == 0 and x % 100 != 0) or (x % 400 == 0):
#         return f"{x} is leap year"
#     else:
#         return f"{x} is not a leap year"
    
# print(leepYear(2001))

# counter = {}
# word = "Spiderman - Great power comes great responsibilities."

# for i in word:
#     if i not in counter:
#         counter[i] = 1
#     else:
#         counter[i] += 1

# print(counter)

# keys = ["a", "b", "c"]
# values = [100, 200, 300]

# dictionary = dict(zip(keys, values))
# print(dictionary)

# salary = [50_000, 60_000, 55_000, 70_000, 52_000]

# maximumSalary = max(salary)
# averageSalary = sum(salary) / len(salary)

# aboveAverage = [i for i in salary if i > averageSalary]

# sortedSalary = sorted(salary, reverse=True)

# print(f"Max Salary: {maximumSalary}")
# print(f"Above Average Salary: {aboveAverage}")
# print(f"Descending order Salary: {sortedSalary}")

# a = [1, 2, 3, 4]
# b = [3, 4, 5, 6]

# setCombined = set(a + b)
# setA = set(a)
# setB = set(b)

# print(f"Removed dupicates: {setCombined}")
# print(f"Difference : {setA.difference(setB)} and {setB.difference(setA)}")

# class Employee:
#     def __init__(self, name, salary):
#         self.name = name
#         self.salary = salary

#     def display(self):
#         print(f"Name: {self.name}, Salary: {self.salary}")

#     def is_high_earner(self):
#         if self.salary > 60_000:
#             print(f"{self.name} is high earner")
#         else:
#             print(f"{self.name} is not a high earner")

# class Project(Employee):
#     def __init__(self, name, salary, project_name, hours_allocated):
#         super().__init__(name, salary)
#         self.project_name = project_name
#         self.hours_allocated = hours_allocated

#     def display(self):
#         print(f"Name: {self.name}, Salary: {self.salary}, Project Name: {self.project_name}, Hours: {self.hours_allocated}")

# p1 = Project("Tharun", 45_000, "Yolo detection", 4.5)
# p1.display()
# p1.is_high_earner()

# e1 = Employee("Tharun", 45_000)
# e2 = Employee("Suriya", 89_000)
# e3 = Employee("Ajith Kumar", 1_30_000)

# e1.is_high_earner()
# e2.is_high_earner()
# e3.is_high_earner()

# employees = [
#     {"name": "Ali", "department": "HR"},
#     {"name": "Neha", "department": "IT"},
#     {"name": "Ravi", "department": "Finance"},
#     {"name": "Sara", "department": "IT"},
#     {"name": "Vikram", "department": "HR"}
# ]

# with open(r"C:\Stack overflow\Data-Warehouse-Training\May-30\Code\IT.txt", "w") as f:
#     for i in employees:
#         if i["department"] == "IT":
#             f.write(i["name"] + "\n")

# with open(r"C:\Stack overflow\Data-Warehouse-Training\May-30\Code\dialogue.txt", "r") as f:
#     content = f.read()
#     count = len(content.split(" "))

# print(f"Number of words: {count}")

# try:
#     num = int(input("Enter a number: "))

#     if not isinstance(num, (int, float)):
#         raise ValueError
#     print(num ** 2)


# except ValueError as e:
#     print(f"Error: {e}")

# def division(x, y):
#     if y == 0:
#         raise ZeroDivisionError("Division by zero!!")
#     return x / y

# try: 
#     x = int(input("Enter X: "))
#     y = int(input("Enter Y: "))
#     div = division(x, y)
#     print(div)

# except ZeroDivisionError as e:
#     print(f"Error: {e}")

import pandas as pd

dfEmp = pd.read_csv(r"C:\Stack overflow\Data-Warehouse-Training\May-30\Code\employees.csv")
dfPro = pd.read_csv(r"C:\Stack overflow\Data-Warehouse-Training\May-30\Code\projects.csv")

# print(dfEmp.head(2))
# print(dfPro.head(2))

# print(dfEmp["Department"].value_counts())
# print(dfEmp.groupby(by="Department").agg({"Salary": "median"}))

# dfEmp["JoiningDate"] = pd.to_datetime(dfEmp["JoiningDate"])
# dfEmp["TenureInYears"] = pd.Timestamp.now().year - dfEmp["JoiningDate"].dt.year

# print(dfEmp)

# deptIT = dfEmp.query("Department == 'IT' & Salary >= 60000")
# print(deptIT)

# dfEmpFiltered = dfEmp.groupby(by="Department").agg(
#                                                     employeeCount = ("Name", "count"),
#                                                     totalSalary = ("Salary", "sum"),
#                                                     averageSalary = ("Salary", "median")
#                                                     )

# print(dfEmpFiltered)

# print(dfEmp.sort_values(by="Salary", ascending=False))

# merged = dfEmp.merge(dfPro, on="EmployeeID", how="inner")
# print(merged[["Name", "Department", "ProjectName", "HoursAllocated"]])

# leftJoin = dfEmp.merge(right=dfPro, on="EmployeeID", how="left")
# filteredLeftJoin = leftJoin[leftJoin["ProjectID"].isnull()]
# print(filteredLeftJoin)

merged = dfEmp.merge(dfPro, on="EmployeeID", how="inner")
merged["TotalCost"] = merged["HoursAllocated"] * (merged["Salary"] / 160)

print(merged)