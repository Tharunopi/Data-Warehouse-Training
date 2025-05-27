# num = input("Enter a number: ")
# num = [int(i) for i in num]
# digitSum = sum(num)
# print(digitSum)

# num = input("Enter 3 digit number: ")
#
# if len(num) != 3:
#     print("Enter a 3 digit number!")
# else:
#     print(num[::-1])

# metres = int(input("Enter measurement in metres: "))
# cm = metres * 100
# feet = metres * 3.281
# inches = metres * 39.37
#
# print(f"{metres} meter = {cm} cm, {feet} feet, {inches} inches")

# m1 = int(input("Enter Mark-1: "))
# m2 = int(input("Enter Mark-2: "))
# m3 = int(input("Enter Mark-3: "))
# m4 = int(input("Enter Mark-4: "))
# m5 = int(input("Enter Mark-5: "))
#
# total = m1 + m2 + m3 + m4 + m5
# average = total / 5
# percentage = (total / 500) * 100
#
# print(f"Total: {total}, Average: {average}, Percentage: {percentage}")

# year = int(input("Enter year: "))
#
# if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
#     print(f"{year} is leap year")
# else:
#     print(f"{year} is non-leap year")

# x = int(input("Enter x: "))
# operator = input("Enter operator: ")
# y = int(input("Enter y: "))
#
# if operator == '+':
#     print(x + y)
# elif operator == '-':
#     print(x - y)
# elif operator == '*':
#     print(x * y)
# elif operator == '/' and y != 0:
#     print(x / y)
# else:
#     print("Enter a valid number or operator")

# len1 = int(input("Enter length 1: "))
# len2 = int(input("Enter length 2: "))
# len3 = int(input("Enter length 3: "))
#
# if ((len2 + len3) > len1) and ((len1 + len3) > len2) and ((len2 + len1) > len3):
#     print("This is a valid triangle")
# else:
#     print("Invalid triangle")

# billAmount = int(input("Enter total bill amount: "))
# numberOfPeople = int(input("Enter number of people: "))
# tipPercent = float(input("Enter tip percentage: ")) / 100
#
# totalAmount = billAmount + (billAmount * tipPercent)
# perPerson = totalAmount / numberOfPeople
# print(f"Final amount per person: {perPerson}")

# from math import sqrt
#
# for i in range(2, 101):
#     prime = True
#     for j in range(2, int(i ** 0.5) + 1):
#         if i % j == 0:
#             prime = False
#             break
#     if prime:
#         print(f"{i} is a prime")

# x = input("Enter a string: ")
# y = x
# palindrome = True
#
# for i in range(len(x)):
#     if x[i] != y[len(x) -1 -i]:
#         palindrome = False
#         break
#
# print(f"{x} is palindrome - {palindrome}")

# num = int(input("Enter number: "))
#
# values = [0, 1]
#
# for i in range(2, num+1):
#     values.append(values[i-1] + values[i-2])
#
# print(values)

# num = int(input("Enter number for table: "))
#
# for i in range(10):
#     print(f"{i+1} * {num} = {(i+1) * num}")

# import random
#
# num = random.randint(1, 100)
# guess = False
#
# while not guess:
#     userGuess = int(input("Enter guess"))
#     if userGuess == num:
#         print("Correct")
#         guess = True
#
#     elif userGuess < num:
#         print("Too Low")
#
#     else:
#         print("Too High")

# balance = 10_000
# online = True
#
# while online:
#     print(f"1. Deposit\n2. Withdraw\n3. Check Balance\n4. Exit")
#     num = int(input("Enter choice: "))
#
#     if num == 1:
#         depositAmt = int(input("Enter deposit amount: "))
#         balance += depositAmt
#
#     elif num == 2:
#         withdrawAmt = int(input("Enter withdraw amount"))
#         if withdrawAmt <= balance:
#             balance -= withdrawAmt
#         else:
#             print("Not sufficient balance")
#
#     elif num == 3:
#         print(balance)
#
#     else:
#         online = False
# import string
#
# password = input("Enter password: ")
# chars, number, capital, symbol = False, False, False, False
#
# if len(password) >= 8:
#     chars = True
#
# for i in password:
#     if i.isdigit():
#         number = True
#     elif i.isupper():
#         capital = True
#     elif i in string.punctuation:
#         symbol = True
#
# if all([chars, number, capital, symbol]):
#     print("Strong password")
# else:
#     print("Weak password")

a = int(input("Enter a: "))
b = int(input("Enter b: "))

while b!= 0:
    a, b = b, a%b

print(f"GCD: {a}")