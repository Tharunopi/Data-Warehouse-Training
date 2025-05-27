student = {
    "name": "Ayyan",
    "age": 21,
    "dept": "CSE"
}

print(student)

student["age"] = 23
student["city"] = "Hyderabad"

for i,j in student.items():
    print(i, "->", j)