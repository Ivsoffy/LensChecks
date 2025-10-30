import pandas as pd
from sklearn.preprocessing import StandardScaler
import re
import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import scatter_matrix
import numpy as np
import os
import warnings


# Suppress the specific DeprecationWarning
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*DataFrameGroupBy.apply operated on the grouping columns.*")


# Log function to decrase the tails

def log_function(df_in, value_col, final_col):
    """
    Apply log function to the data
    """
    df_in[final_col] = np.log(df_in[value_col])
    return df_in


# Z-scaling

def normalize(df, group_cols, value_col, final_col):
    """
    Apply z-score normalization to a specified column within groups and add the resulting column to the original DataFrame.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the data.
    group_cols (list): The columns to group by for normalization (e.g., ['Company', 'Region']).
    value_col (str): The column to be z-score normalized (e.g., 'Scaled_Base_Pay').
    final_col (str): The name of the final column to store the normalized values.

    Returns:
    pd.DataFrame: The DataFrame with the new normalized column.
    """
    def zscore_group(group_df):
        scaler = StandardScaler()
        group_df[final_col] = scaler.fit_transform(group_df[[value_col]])
        return group_df
    
    # Apply the z-score normalization within each group
    df = df.groupby(group_cols).apply(zscore_group).reset_index(drop=True)
    
    return df


def calculate_f(df):

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
        'Выручка категория': 'revenue_cat',
    }, inplace=True)

    df['spec'] = df['spec'].fillna('-')

   
    # logging the Base Pay before Scaling
    df = log_function(df, 'BP', 'Logged_BP')
    
    # Scaling Logged Base Pay for the whole company (scaling to std=1 and mean=0 for each worker)
    df = normalize(df, ['company'], 'Logged_BP', 'Scaled_Logged_BP')
    
    # Scaling Logged Base Pay for the whole company on Regions
    df = normalize(df, ['company', 'region'], 'Logged_BP', 'Scaled_Logged_BP_Region')
    
    df = df.drop(['Logged_BP'], axis=1)

    
    # Salary to headcount ratio: All company
    grouped = df.groupby('company').agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_C'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'STH_C']], on='company', how='left')
    
    
    # Salary to headcount ratio: company and function
    grouped = df.groupby(['company', 'function']).agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_F'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'STH_F']], on=['company', 'function'], how='left')
    
    
    # Salary to headcount ratio: company and subfunction
    grouped = df.groupby(['company', 'function', 'subfunction']).agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_SU'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'subfunction', 'STH_SU']], on=['company', 'function', 'subfunction'], how='left')
    
    
    # Salary to headcount ratio: company and subfunction
    grouped = df.groupby(['company', 'function', 'subfunction', 'spec']).agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_SP'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'subfunction', 'spec', 'STH_SP']], on=['company', 'function', 'subfunction', 'spec'], how='left')
    
    
    # Salary to headcount ratio: All company
    grouped = df.groupby(['company', 'region']).agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_C_R'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'region', 'STH_C_R']], on=['company', 'region'], how='left')
    
    
    # Salary to headcount ratio: company and function
    grouped = df.groupby(['company', 'function', 'region']).agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_F_R'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'region', 'STH_F_R']], on=['company', 'function', 'region'], how='left')
    
    
    # Salary to headcount ratio: company and subfunction
    grouped = df.groupby(['company', 'function', 'subfunction', 'region']).agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_SU_R'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'subfunction', 'region', 'STH_SU_R']], on=['company', 'function', 'subfunction', 'region'], how='left')
    
    
    # Salary to headcount ratio: company and subfunction
    grouped = df.groupby(['company', 'function', 'subfunction', 'spec', 'region']).agg(
        total_salary=('BP', 'sum'),
        headcount=('job_title', 'count')
    ).reset_index()
    
    # Calculate the salary to headcount ratio
    grouped['STH_SP_R'] = grouped['total_salary'] / grouped['headcount']
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'subfunction', 'spec', 'region', 'STH_SP_R']], on=['company', 'function', 'subfunction', 'spec', 'region'], how='left')

    # df = normalize (df, ['company'], 'STH_C', 'Initial_S_STH_C')
    # df = normalize (df, ['company'], 'STH_F', 'Initial_S_STH_F')
    # df = normalize (df, ['company'], 'STH_SU', 'Initial_S_STH_SU')
    # df = normalize (df, ['company'], 'STH_SP', 'Initial_S_STH_SP')
    
    # df = normalize (df, ['company'], 'STH_C_R', 'Initial_S_STH_C_R')
    # df = normalize (df, ['company'], 'STH_F_R', 'Initial_S_STH_F_R')
    # df = normalize (df, ['company'], 'STH_SU_R', 'Initial_S_STH_SU_R')
    # df = normalize (df, ['company'], 'STH_SP_R', 'Initial_S_STH_SP_R')
    
    # df = normalize(df, ['company'], 'STH_C', 'S_STH_C')
    # df = normalize(df, ['company', 'function'], 'STH_F', 'S_STH_F')
    # df = normalize(df, ['company', 'function', 'subfunction'], 'STH_SU', 'S_STH_SU')
    # df = normalize(df, ['company', 'function', 'subfunction', 'spec'], 'STH_SP', 'S_STH_SP')
    
    # df = normalize(df, ['company', 'region'], 'STH_C_R', 'S_STH_C_R')
    # df = normalize(df, ['company', 'function', 'region'], 'STH_F_R', 'S_STH_F_R')
    # df = normalize(df, ['company', 'function', 'subfunction', 'region'], 'STH_SU_R', 'S_STH_SU_R')
    # df = normalize(df, ['company', 'function', 'subfunction', 'spec', 'region'], 'STH_SP_R', 'S_STH_SP_R')
    
    
    # # Drop unused columns
    # df = df.drop(['STH_C', 'STH_F', 'STH_SU', 'STH_SP', 'STH_C_R', 'STH_F_R', 'STH_SU_R', 'STH_SP_R'], axis=1)
    
    
    # Method 1: Using groupby and size
    employee_count_size = df.groupby('company').size().reset_index(name='headcount')
    
    df = df.merge(employee_count_size, on='company', how='left')
    
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function']).agg(
        headcount_fun=('job_title', 'count')
    ).reset_index()


    
    # grouped = normalize(grouped, ['company', 'function'], 'headcount_fun', 'Scaled_HF')
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'headcount_fun']], on=['company', 'function'], how='left')
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function', 'subfunction']).agg(
        headcount_sub=('job_title', 'count')
    ).reset_index()
    
    # grouped = normalize(grouped, ['company', 'function', 'subfunction'], 'headcount_sub', 'Scaled_HSU')
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'subfunction', 'headcount_sub']], on=['company', 'function', 'subfunction'], how='left')
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function', 'subfunction', 'spec']).agg(
        headcount_spec=('job_title', 'count')
    ).reset_index()
    
    # grouped = normalize(grouped, ['company', 'function', 'subfunction', 'spec'], 'headcount_spec', 'Scaled_HSP')
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped[['company', 'function', 'subfunction', 'spec', 'headcount_spec']], on=['company', 'function', 'subfunction', 'spec'], how='left')
    
    
    df['FtC'] = df['headcount_fun'] / df['headcount']
    df['SUtC'] = df['headcount_sub'] / df['headcount']
    df['SPtC'] = df['headcount_spec'] / df['headcount']
    
    df['SUtF'] = df['headcount_sub'] / df['headcount_fun']
    df['SPtSU'] = df['headcount_spec'] / df['headcount_sub']
    
    
    # Drop unused columns
    df = df.drop(['headcount','headcount_fun','headcount_sub','headcount_spec'], axis=1)
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company']).agg(
        functions_num=('function', lambda x: len(pd.unique(x)))
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped, on=['company'], how='left')
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function']).agg(
        subfunctions_num=('subfunction', lambda x: len(pd.unique(x)))
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped, on=['company', 'function'], how='left')
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function', 'subfunction']).agg(
        spec_num=('spec', lambda x: len(pd.unique(x)))
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped, on=['company', 'function', 'subfunction'], how='left')
    
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company']).agg(
        salary_sum_comp=('BP', 'sum')
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped, on=['company'], how='left')
    
    df["EmpBP_Portion_C"] = (df["BP"] / df["salary_sum_comp"])
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function']).agg(
        salary_sum_fun=('BP', 'sum')
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped, on=['company', 'function'], how='left')
    
    df["EmpBP_Portion_F"] = (df['BP'] / df["salary_sum_fun"])
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function', 'subfunction']).agg(
        salary_sum_subfun=('BP', 'sum')
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped, on=['company', 'function', 'subfunction'], how='left')
    
    df["EmpBP_Portion_SU"] = df["BP"] / df["salary_sum_subfun"]
    
    
    # Group by company and calculate total salary and headcount for all the company
    grouped = df.groupby(['company', 'function', 'subfunction', 'spec']).agg(
        salary_sum_spec=('BP', 'sum')
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(grouped, on=['company', 'function', 'subfunction', 'spec'], how='left')
    
    df["EmpBP_Portion_SP"] = df["BP"] / df["salary_sum_spec"]
    
    
    df = log_function(df, 'EmpBP_Portion_C', "Logged_EmpBP_Portion_C")
    df = normalize(df, ['company'], "Logged_EmpBP_Portion_C", 'Scaled_EmpBP_Portion_C')
    
    df = log_function(df, 'EmpBP_Portion_F', "Logged_EmpBP_Portion_F")
    df = normalize(df, ['company', 'function'], "Logged_EmpBP_Portion_F", 'Scaled_EmpBP_Portion_F')
    
    df = log_function(df, "EmpBP_Portion_SU", "Logged_EmpBP_Portion_SU")
    df = normalize(df, ['company', 'function', 'subfunction'], "Logged_EmpBP_Portion_SU", 'Scaled_EmpBP_Portion_SU')
    
    df = log_function(df, "EmpBP_Portion_SP", "Logged_EmpBP_Portion_SP")
    df = normalize(df, ['company', 'function', 'subfunction', 'spec'], "Logged_EmpBP_Portion_SP", 'Scaled_EmpBP_Portion_SP')
    
    df = df.drop(["EmpBP_Portion_C", "EmpBP_Portion_F", "EmpBP_Portion_SU", "EmpBP_Portion_SP",
                 "Logged_EmpBP_Portion_C", "Logged_EmpBP_Portion_F", "Logged_EmpBP_Portion_SU",
                 "Logged_EmpBP_Portion_SP"], axis=1)
    
    
    number_1 = df.groupby(['company', 'function', 'job_title']).agg(
        emp_in_job=('BP', "count")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(number_1, on=['company', 'function', 'job_title'], how='left')
    
    number_2 = df.groupby(['company', 'function', 'region', 'job_title']).agg(
        emp_in_job_r=('BP', "count")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(number_2, on=['company', 'function', 'region', 'job_title'], how='left')
    
    df = log_function(df, 'emp_in_job', 'Logged_emp_in_job')
    df = log_function(df, 'emp_in_job_r', 'Logged_emp_in_job_r')
    
    df = normalize(df, ['company'], 'Logged_emp_in_job', 'Scaled_emp_in_job')
    df = normalize(df, ['company', 'region'], 'Logged_emp_in_job_r', 'Scaled_emp_in_job_r')
    
    df = df.drop(['emp_in_job', 'emp_in_job_r', 'Logged_emp_in_job', 'Logged_emp_in_job_r'], axis=1)
    
    
    median = df.groupby(['company']).agg(
        median_comp=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company'], how='left')
    
    median = df.groupby(['company', 'function']).agg(
        median_fun=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company', 'function'], how='left')
    
    median = df.groupby(['company', 'function', 'subfunction']).agg(
        median_su=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company', 'function', 'subfunction'], how='left')
    
    median = df.groupby(['company', 'function', 'subfunction', 'spec']).agg(
        median_sp=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company', 'function', 'subfunction', 'spec'], how='left')
    
    median = df.groupby(['company', 'region']).agg(
        median_comp_r=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company', 'region'], how='left')
    
    median = df.groupby(['company', 'function', 'region']).agg(
        median_fun_r=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company', 'function', 'region'], how='left')
    
    median = df.groupby(['company', 'function', 'subfunction', 'region']).agg(
        median_su_r=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company', 'function', 'subfunction', 'region'], how='left')
    
    median = df.groupby(['company', 'function', 'subfunction', 'spec', 'region']).agg(
        median_sp_r=('BP', "median")
    ).reset_index()
    
    # Merge the ratio back to the original DataFrame
    df = df.merge(median, on=['company', 'function', 'subfunction', 'spec', 'region'], how='left')
    
    df['CR_C'] = df['BP'] / df['median_comp']
    df['CR_F'] = df['BP'] / df['median_fun']
    df['CR_SU'] = df['BP'] / df['median_su']
    df['CR_SP'] = df['BP'] / df['median_sp']
    df['CR_C_R'] = df['BP'] / df['median_comp_r']
    df['CR_F_R'] = df['BP'] / df['median_fun_r']
    df['CR_SU_R'] = df['BP'] / df['median_su_r']
    df['CR_SP_R'] = df['BP'] / df['median_sp_r']
    
    df = log_function(df, 'CR_C', 'Logged_CR_C')
    df = log_function(df, 'CR_F', 'Logged_CR_F')
    df = log_function(df, 'CR_SU', 'Logged_CR_SU')
    df = log_function(df, 'CR_SP', 'Logged_CR_SP')
    df = log_function(df, 'CR_C_R', 'Logged_CR_C_R')
    df = log_function(df, 'CR_F_R', 'Logged_CR_F_R')
    df = log_function(df, 'CR_SU_R', 'Logged_CR_SU_R')
    df = log_function(df, 'CR_SP_R', 'Logged_CR_SP_R')
    
    df = normalize(df, ['company'], 'Logged_CR_C', 'Scaled_CR_C')
    df = normalize(df, ['company', 'function'], 'Logged_CR_F', 'Scaled_CR_F')
    df = normalize(df, ['company', 'function', 'subfunction'], 'Logged_CR_SU', 'Scaled_CR_SU')
    df = normalize(df, ['company', 'function', 'subfunction', 'spec'], 'Logged_CR_SP', 'Scaled_CR_SP')
    df = normalize(df, ['company', 'region'], 'Logged_CR_C_R', 'Scaled_CR_C_R')
    df = normalize(df, ['company', 'function', 'region'], 'Logged_CR_F_R', 'Scaled_CR_F_R')
    df = normalize(df, ['company', 'function', 'subfunction', 'region'], 'Logged_CR_SU_R', 'Scaled_CR_SU_R')
    df = normalize(df, ['company', 'function', 'subfunction', 'spec', 'region'], 'Logged_CR_SP_R', 'Scaled_CR_SP_R')
    
    df = df.drop(['median_comp', 'median_fun', 'median_su', 'median_sp', 'median_comp_r', 'median_fun_r', 'median_su_r', 'median_sp_r', 
                 'Logged_CR_C', 'Logged_CR_F', 'Logged_CR_SU', 'Logged_CR_SP', 'Logged_CR_C_R', 'Logged_CR_F_R', 'Logged_CR_SU_R', 'Logged_CR_SP_R',
                 'CR_C', 'CR_F', 'CR_SU', 'CR_SP', 'CR_C_R', 'CR_F_R', 'CR_SU_R', 'CR_SP_R'], axis=1)

    def sanitize_text(text):
        text = text.lower()
        text = re.sub(r'[^a-zа-я0-9\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    # Apply the sanitize function to the relevant columns
    df['job_title'] = df['job_title'].apply(sanitize_text)
    df['industry'] = df['industry'].apply(sanitize_text)
    df['region'] = df['region'].apply(sanitize_text)
    df['headcount_cat'] = df['headcount_cat'].apply(sanitize_text)
    df['revenue_cat'] = df['revenue_cat'].apply(sanitize_text)
    df['function'] = df['function'].str.strip().str.upper()
    df['subfunction'] = df['subfunction'].str.strip().str.upper()
    df['spec'] = df['spec'].str.strip().str.upper()

    return df