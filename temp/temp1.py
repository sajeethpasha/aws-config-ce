import pandas as pd

df = pd.DataFrame({"col1": ["apple", "banana", "cherry"],
                   "col2": ["orange", "grape", "pineapple"]})

df_result = df.apply(lambda x: x[:3])
print(df_result)