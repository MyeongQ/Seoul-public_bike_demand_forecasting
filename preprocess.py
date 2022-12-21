import chardet as chardet
import pandas as pd
import numpy as np
import os
import dask.dataframe as dd
from dask.dataframe.io.io import from_map


def read_excel(inputs, **kwargs):
    return from_map(pd.read_excel, inputs, **kwargs)


dataset_name_list = [
    "서울특별시 공공자전거 이용정보(시간대별)",
    "서울특별시 공공자전거 일별 대여건수",
    "서울특별시 기상정보",
    "서울특별시 공공자전거 대여이력"
]

PATH = os.path.join(os.path.curdir, "Dataset")

target_set = "서울특별시 공공자전거 대여이력"
target_path = os.path.join(PATH, target_set)

file_list = os.listdir(target_path)
## file_list.sort(reverse=True)
print(file_list)



df = None
for idx, fname in enumerate(file_list):
    print("file_", idx, "file name: ", fname)
    if df is None:
        if "xlsx" in fname:
            df = pd.read_excel(os.path.join(target_path, fname), dtype=str)
        elif "csv" in fname:
            try:
                df = pd.read_csv(os.path.join(target_path, fname), engine='python', encoding="cp949", dtype=str,
                                 error_bad_lines=False)
            except UnicodeDecodeError:
                df = pd.read_csv(os.path.join(target_path, fname), engine='python', encoding="utf-8", dtype=str,
                                 error_bad_lines=False)
        else:
            continue
        df.columns = ["자전거번호", "대여일시", "대여대여소", "대여대여소이름", "대여거치대", "반납일시", "반납대여소", "반납대여소이름", "반납거치대", "이용시간", "이용거리"]
        df['대여일시'] = pd.to_datetime(df['대여일시'])
        df['반납일시'] = pd.to_datetime(df['반납일시'])

    else:
        if "xlsx" in fname:
            temp = pd.read_excel(os.path.join(target_path, fname), dtype=str)
        elif "csv" in fname:
            try:
                temp = pd.read_csv(os.path.join(target_path, fname), engine='python', encoding="utf-8",
                                   error_bad_lines=False, dtype=str)
            except UnicodeDecodeError:
                temp = pd.read_csv(os.path.join(target_path, fname), engine='python', encoding="cp949",
                                   error_bad_lines = False, dtype=str)

        else:
            continue

        temp.columns = ["자전거번호", "대여일시", "대여대여소", "대여대여소이름", "대여거치대", "반납일시", "반납대여소", "반납대여소이름", "반납거치대", "이용시간", "이용거리"]
        temp['대여일시'] = pd.to_datetime(temp['대여일시'])
        temp['반납일시'] = pd.to_datetime(temp['반납일시'])
        df = pd.concat([df, temp], axis=0)
    print(df.tail()['반납일시'])

if target_set == "서울특별시 기상정보":
    # 인덱스 제거
    df.reset_index(drop=True, inplace=True)

    # 지점, 지점명 칼럼 제거
    df.drop(columns=["지점", "지점명"], inplace=True)

    # 결측치 제거
    df["강수량(mm)"] = df["강수량(mm)"].fillna(0)
    df["적설(cm)"] = df["적설(cm)"].fillna(0)
    print("===Interpolate Missing Values===")
    df['기온(°C)'].interpolate(method='linear', axis=0, inplace=True)

    # CSV 저장
    df.to_csv(os.path.join(target_path, "서울_기상정보.csv"))

elif target_set == "서울특별시 공공자전거 이용정보(시간대별)":
    df.reset_index(drop=True, inplace=True)
    df.columns = ["대여일자", "대여시간", "대여소번호", "대여소명", "대여구분코드", "성별", "연령대코드", "이용건수", "운동량", "탄소량", "이동거리", "사용시간"]
    #df.columns = ["대여일자", "대여시간", "대여소번호", "대여소명", "대여구분코드", "성별", "연령대코드", "이용건수", "이동거리", "사용시간"]
    df.drop(columns=['운동량', '탄소량'], inplace=True)
    df['대여일자'] = pd.to_datetime(df['대여일자'])
    df.to_csv(os.path.join(target_path, "서울특별시 공공자전거 이용정보(시간대별)_2022.csv"), encoding="utf-8", index=False)

elif target_set == "서울특별시 공공자전거 대여이력":
    df.reset_index(drop=True, inplace=True)
    df.columns = ["자전거번호", "대여일시", "대여대여소", "대여대여소이름", "대여거치대", "반납일시", "반납대여소", "반납대여소이름", "반납거치대", "이용시간", "이용거리"]
    df.drop(columns=['자전거번호'], inplace=True)
    df['대여일시'] = pd.to_datetime(df['대여일시'])
    df['반납일시'] = pd.to_datetime(df['반납일시'])
    df.to_csv(os.path.join(target_path, "서울특별시 공공자전거 대여이력_2022.csv"), encoding="utf-8", index=False)

print(df.head())
print(df.info())
print(df.describe())
print(df.isnull().sum())

