import pandas as pd

data = {
  "calories": [420, 420,420,420],
  "duration": [50,50, 50, 50]
}

#load data into a DataFrame object:
df = pd.DataFrame(data)

# print(df) 

# frames = [process_your_file(f) for f in files]
dfs= pd.concat([df,df,df],ignore_index=True)
print(dfs)