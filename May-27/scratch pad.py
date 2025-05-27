#filters

# num = [i+1 for i in range(20)]
#
# even = list(filter(lambda x: x % 2 == 0, num))
# print(even)

# map

# num = [1, 2, 3, 4, 5]
#
# def power(val: int) -> int:
#     return val * val
#
# powered = list(map(power, num))
# squared = list(map(lambda x: x ** 2, num))
#
# print(powered)
# print(squared)

#generators
#
# text = ["hi", "this", "is", "tharun"]
#
# def looper(x):
#     for i in text:
#         yield i
#
# ans = looper(text)
#
# for i in ans:
#     print(i)

#decrator

def reducer(func):

    def wrapper():
        