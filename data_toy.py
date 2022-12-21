import pandas as pd
import numpy as np
import dask.dataframe as dd
import os

path = os.path.join(os.path.curdir, 'Dataset/서울특별시 공공자전거 대여이력')
df = pd.read_csv(os.path.join(path, "서울특별시 공공자전거 대여이력_2019.csv"), engine='python', encoding='utf-8', dtype=str)

print(df.info())
print("=======")
print(df.isnull().sum())
print("=======")
for col in df.columns:
    print(col, df[col].nunique())


print(df.head())
print(df.tail())


#df.to_csv(os.path.join(path, "서울특별시 공공자전거 이용정보(시간대별)_2018.csv"), encoding='utf-8', index=False)



