# file = open("C:\Stack overflow\Data-Warehouse-Training\May-29\Code\sample.txt", "r")
# content = file.read()
# print(content)
# file.close()


with open("C:\Stack overflow\Data-Warehouse-Training\May-29\Code\sample.txt", "w") as f:
    f.write("Hi this is Tharun\n")
    f.write("Tharun Atithya is my name")

with open("C:\Stack overflow\Data-Warehouse-Training\May-29\Code\sample.txt", "r") as f:
    content = f.read()
    print(content)