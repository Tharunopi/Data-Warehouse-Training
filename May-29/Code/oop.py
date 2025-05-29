# class is like giving eyes to compiler 

class Student:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def greet(self):
        print(f"Hello! I am {self.name} and I am {self.age} years old.")

s1 = Student("Tharun", 21)
s1.greet()