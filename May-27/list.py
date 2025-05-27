values = [i+1 for i in range(20)]

for i in values:
    if i % 2 == 0:
        print(f"{i} is Even")
    else:
        print(f"{i} is Odd")