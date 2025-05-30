import numpy as np
import pandas as pd

arr = np.array([1, 2, 3, 4])
print(arr * 2)
print(np.mean(arr))

data = {
    "Name": ["Ali", "Neha", "Ravi"],
    "Marks": [85, 90, 88]
}

df = pd.DataFrame(data, index=[100, 101, 102])
print(df)