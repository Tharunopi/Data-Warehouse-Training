# x = [(i + 1) * (i + 1) for i in range(20)]
# print(x)

# x = [9, 4, 6, 78, 56, 129, 897]


# for i in range(2):
#     largest = x[0]
#     for j in x:
#         if j > largest:
#             largest = j
#     ans = x.remove(largest)

# print(largest)

# x = [1, 2, 3, 4 ,3, 2, 1]
# unique = []

# for i in x:
#     if i not in unique:
#         unique.append(i)
# print(unique)

# steps = 2

# x = [1, 2, 3, 4, 5]
# print(x[steps+1:] + [x[len(x)//2]] + x[:steps])

# x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# y = [i*2 for i in x if i%2 == 0]
# print(y)

# def swapper(x: tuple, y:tuple) -> tuple:
#     x, y = y, x
#     return x, y

# a = (1, 2, 3)
# b = (4, 5 ,6)

# print(swapper(a, b))

# x = ("Tharun", 21, "AI&DS", "O+")

# print(f"Name: {x[0]}, Age: {x[1]}, Branch: {x[2]}, Grade: {x[3]}.")

# x = (("a", 1), ("b", 2))
# y = {}

# for i, j in x:
#     y[i] = j

# print(y)

# x = [1, 2, 3, 4]
# y = [3, 4, 5, 6]

# x, y = set(x), set(y)

# print(x.intersection(y))

# sentence = """Python is very fun to learn. Beacause it is Python"""
# x = set(sentence.split(" "))
# print(x)

# x = {1, 2, 3, 4}
# y = {1, 2, 3}

# print(y.issubset(x))

# sentence = """Python is very fun to learn. Beacause it is Python"""
# freq = {}

# for i in sentence:
#     if i not in freq:
#         freq[i] = 1
#     else:
#         freq[i] += 1

# print(freq)

# def grader(x):
#     if x >= 90: 
#         return "A" 
#     elif x >= 75:
#         return "B"
#     else: 
#         return "C"

# students = {}

# for i in range(3):
#     name = input("Enter your name: ")
#     mark = int(input("Enter your mark: "))
#     grade = grader(mark) 
#     students[name] = [mark, grade]

# print("_____")

# for i in  range(3):
#     name = input("Enter your name to get grade: ")
#     print(f"Name: {name}, Grade: {students[name][1]}")

# x = {"a": 1, "b": 2}
# y = {"a": 3, "c": 9}

# for i, j in y.items():
#     if i in x:
#         x[i] += j
#     else:
#         x[i] = j

# print(x)

# x = {"a": 1, "b": 2}
# y = {}

# for i,j in x.items():
#     y[j] = i

# print(y)

# words = ['Bleach', 'follows', 'Ichigo', 'Kurosaki,', 'a', 'teenager', 'who', 'can', 'see', 'ghosts,', 'as', 'he', 'becomes', 'a', 'Soul', 'Reaper', 'after', 'a', 'Soul', 'Reaper,', 'Rukia', 'Kuchiki,', 'transfers', 'her', 'powers', 'to', 'him', 'to', 'fight', 'Hollows.', 'Ichigo,', 'along', 'with', 'Rukia', 'and', 'other', 'friends,', 'is', 'tasked', 'with', 'defending', 'humans', 'from', 'Hollows,', 'guiding', 'souls', 'to', 'the', 'afterlife,', 'and', 'maintaining', 'the', 'balance', 'between', 'the', 'living', 'and', 'the', 'dead.']
# grouper = {}

# for i in words:
#     if len(i) in grouper:
#         grouper[len(i)].append(i)
#     else:
#         grouper[len(i)] = [i]

# print(grouper)