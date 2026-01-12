import numpy as np
import pandas as pd
import re
import warnings
import sys
import os

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message=".*DataFrameGroupBy.apply operated on the grouping columns.*",
)

def log_function(df_in, value_col, final_col):
    df_in[final_col] = np.log(df_in[value_col])
    return df_in


def normalize(df, group_cols, value_col, final_col):
    mean = df.groupby(group_cols)[value_col].transform("mean")
    std = df.groupby(group_cols)[value_col].transform(lambda x: x.std(ddof=0))
    df[final_col] = np.where(std == 0, 0.0, (df[value_col] - mean) / std)
    return df


def add_salary_to_headcount_ratio(df, group_cols, out_col):
    grouped = df.groupby(group_cols).agg(
        total_salary=("BP", "sum"),
        headcount=("job_title", "count"),
    ).reset_index()
    grouped[out_col] = grouped["total_salary"] / grouped["headcount"]
    return df.merge(grouped[group_cols + [out_col]], on=group_cols, how="left")


def add_company_salary_sum(df):
    grouped = df.groupby(["company"]).agg(salary_sum_comp=("BP", "sum")).reset_index()
    return df.merge(grouped, on=["company"], how="left")


def calculate_f_new(df):
    df.rename(columns={
        'Название компании (заполняется автоматически)': 'company',
        'Сектор': 'industry',
        'Общее количество сотрудников по состоянию на 1 мая 2025 года':'headcount_cat',
        'Выручка за 2024 год, руб.': 'revenue_cat',
        'Название должности': 'job_title',
        'Регион/область (заполняется автоматически)': 'region',
        'Грейд / Уровень обзора': 'grade',
        'Код функции': 'function',
        'Код подфункции': 'subfunction',
        'Код специализации': 'spec',
        'Базовый оклад (BP)': 'BP',
        'Индустрия': 'industry',
        'Количество сотрудников категория': 'headcount_cat',
        'Выручка категория': 'revenue_cat'
    }, inplace=True)

    df = normalize(df, ['company'], 'BP', 'Scaled_BP')
    df = normalize(df, ['region'], 'BP', 'Scaled_BP_Region')
    df = normalize(df, ['company', 'code'], 'BP', 'Scaled_BP_Code')
    df = normalize(df, ['region', 'code', 'seniority'], 'BP', 'Scaled_BP_rcs')
    df = add_salary_to_headcount_ratio(df, ['company'], 'STH_C')
    df = add_salary_to_headcount_ratio(df, ['company', 'code'], 'STH_SP')
    df = add_salary_to_headcount_ratio(df, ['company', 'region'], 'STH_C_R')
    df = add_company_salary_sum(df)
    df["EmpBP_Portion_C"] = df["BP"] / df["salary_sum_comp"]

    df = log_function(df, 'EmpBP_Portion_C', 'Logged_EmpBP_Portion_C')
    df = normalize(df, ['company'], 'Logged_EmpBP_Portion_C', 'Scaled_EmpBP_Portion_C')

    grouped = df.groupby(['company', 'function']).agg(
        subfunc_num=('subfunction', lambda x: len(pd.unique(x)))
    ).reset_index()
    df = df.merge(grouped, on=['company', 'function'], how='left')

    median = df.groupby(['code', 'region']).agg(
        median_sp_r=('BP', "median")
    ).reset_index()
    df = df.merge(median, on=['code', 'region'], how='left')
    
    df['CR_SP_R'] = df['BP'] / df['median_sp_r']
    df = log_function(df, 'CR_SP_R', 'Logged_CR_SP_R')
    df = normalize(df, ['company', 'code', 'region'], 'Logged_CR_SP_R', 'Scaled_CR_SP_R')
    
    df = df.drop(["EmpBP_Portion_C",'Logged_CR_SP_R', 'CR_SP_R',
                 "Logged_EmpBP_Portion_C", 'BP'], axis=1)
    
    df = df[~((df["company"] == "3Logic_Group") & (df["grade"] < 17))]
    df = df[~((df["company"] == "VK") & (df["grade"] < 18))]
    
    def sanitize_text(text):
        text = text.lower()
        text = re.sub(r'[^a-zа-я0-9\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    
    print("Размер датасета: ",df.shape[0])
    cols = ["job_title", "industry", "region", "headcount_cat", "revenue_cat", "code"]
    bad = {"", "-", "none", "nan", "null"}

    norm = df[cols].astype(str).apply(lambda s: s.str.strip().str.lower())
    bad_mask = norm.isin(bad) | df[cols].isna()
    empty_cols = bad_mask.apply(lambda r: list(r[r].index), axis=1)
    to_drop = bad_mask.any(axis=1)
    print(df.loc[to_drop, cols].assign(empty_cols=empty_cols[to_drop]))
    print("Размер датасета: ", df.shape[0])
    df = df[~to_drop]
    print("Размер датасета после удаления строк с пустыми значениями в обязательных колонках: ", df.shape[0])
    df['job_title_cleaned'] = df['job_title'].apply(sanitize_text)
    df['industry_cleaned'] = df['industry'].apply(sanitize_text)
    df['region_cleaned'] = df['region'].apply(sanitize_text)
    df['headcount_cat_cleaned'] = df['headcount_cat'].apply(sanitize_text)
    df['revenue_cat_cleaned'] = df['revenue_cat'].apply(sanitize_text)
    df['code_cleaned'] = str(df['code']).strip().upper()
    return df

