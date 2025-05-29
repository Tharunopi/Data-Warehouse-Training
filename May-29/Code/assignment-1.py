# def is_prime(n: int):
#     for i in range(2, int(n*0.5) + 1):
#         if n < 2:
#             return False
#         if n % i == 0:
#             return False
#     return True

# for i in range(100):
#     if is_prime(i+1):
#         print("Prime:", i+1)

# def convert_temp(value, unit):
#     if unit == "C":
#         fahern = (value * (9/5)) + 32
#         return fahern
#     else:
#         celsius = (value - 32) * (5 / 9)
#         return celsius
    
# print(convert_temp(56, "F"))

# def factorial(n):
#     if n == 1 or n == 0:
#         return 1
    
#     return n * factorial(n-1)

# print(factorial(5))

# class Rectangle:
#     def __init__(self, length, width):
#         self.length = length
#         self.width = width

#     def area(self):
#         return self.width * self.length
    
#     def perimeter(self):
#         return 2 * (self.length + self.width)
    
#     def is_square(self):
#         if self.length == self.width:
#             return True
#         return False
    
# r1 = Rectangle(3, 23)
# print(r1.area())
# print(r1.perimeter())
# print(r1.is_square())

# class BankAccount:
#     def __init__(self, name, balance):
#         self.name = name
#         self.balance = balance

#     def deposit(self, amt):
#         self.balance += amt

#     def withdraw(self, amt):
#         if self.balance >= amt:
#             self.balance -= amt
#         else:
#             print("Insufficient balance!!")

#     def get_balance(self):
#         return self.balance
    
# b1 = BankAccount("Tharun", 1000)
# b1.deposit(250)
# b1.withdraw(50)
# print(b1.get_balance())

# class Book:
#     def __init__(self, title, author, price, in_stock):
#         self.title = title
#         self.author = author
#         self.price = price
#         self.in_stock = in_stock

#     def sell(self, quantity):
#         if self.in_stock >= quantity:
#             self.in_stock -= quantity
#             print("Done")
#         else:
#             raise ValueError("Out of Stock!!")
        
# b1 = Book("Pytorch", "Daniel Brouke", 1299, 25)
# b1.sell(26)

# class Student:
#     def __init__(self, name, marks:list):
#         self.name = name
#         self.marks = marks

#     def average(self):
#         return sum(self.marks) / len(self.marks)
    
#     def grade(self):
#         average = self.average()

#         if average >= 90:
#             return "A"
#         elif average >= 70:
#             return "B"
#         elif average >= 50:
#             return "C"
#         else:
#             return "F"
        
# s1 = Student("Tharun", [90, 87, 89, 100, 67])
# print(s1.average())
# print(s1.grade())

# class Person:
#     def __init__(self, name, age):
#         self.name = name
#         self.age = age

# class Employee(Person):
#     def __init__(self, name, age, emp_id, salary):
#         super().__init__(name, age)
#         self.emp_id = emp_id
#         self.salary = salary

#     def display_info(self):
#         print(f"Name: {self.name}, Age: {self.age}, ID: {self.emp_id}, Salary: {self.salary}")

# e1 = Employee("Tharun", 21, 1, 45_000)
# e1.display_info()

# class Vehicle:
#     def __init__(self, name, wheels):
#         self.name = name
#         self.wheels = wheels

#     def description(self):
#         print(f"Name: {self.name}")
#         print(f"Wheels: {self.wheels}")

# class Car(Vehicle):
#     def __init__(self, name, wheels, fuel_type):
#         super().__init__(name, wheels)
#         self.fuel_type = fuel_type

#     def description(self):
#         super().description()
#         print(f"Fuel Type: {self.fuel_type}")

# class Bike(Vehicle):
#     def __init__(self, name, wheels, is_geared):
#         super().__init__(name, wheels)
#         self.is_geared = is_geared

#     def description(self):
#         super().description()
#         print(f"Geared: {self.is_geared}")

# c1 = Car("Jeep Compass", 4, "Disel")
# c1.description()

# print("__________________________")

# b1 = Bike("Contiental GT 650", 2, True)
# b1.description()

class Animal:
    def speak(self):
        print("Animal Sound")

class Dog(Animal):
    def speak(self):
        print("Bow wow")

class Cat(Animal):
    def speak(self):
        print("Meow")

class Cow(Animal):
    def speak(self):
        print("Moo...")

animals = (Dog(), Cat(), Cow())

for i in animals:
    i.speak()