try:
    with open("kdr.txt") as f:
        f.close()

except ZeroDivisionError as e:
    print(e)

except ValueError as e:
    print(e)

except FileNotFoundError as e:
    print(f"Error: {e}")

else: 
    print("Done!")

finally:
    print("Finally statement...")