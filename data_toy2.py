import os
import pandas as pd
import datetime as dt

path = os.path.join(os.path.curdir, 'Dataset/서울특별시 공공자전거 대여이력')
file_list = [name for name in os.listdir(path) if '.csv' in name]
print(file_list)
chunksize = 10**6


for file in file_list:
    df = None
    for cnt, chunk in enumerate(pd.read_csv(os.path.join(path, file), chunksize=chunksize, engine='python', encoding='utf-8', dtype=str)):
        if df is None:
            df = chunk
        else:
            df = pd.concat([df, chunk], axis=0)

    print(df.head())
    print(df.tail())
    print(df.isnull().sum())
    for col in df.columns:
        print(df[col].unique())
    print(df.info())

    #df.to_csv(os.path.join(path, file[:-4]+'_.csv'), encoding='utf-8', index=False)

print("Program Termination")
# chunk['대여일자'] = pd.to_datetime(chunk['대여일자'])
