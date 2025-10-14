
# All the variables are imported from LP.py file
import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side, numbers
import os
import sys
import warnings
import re
from openpyxl import utils
import warnings
# warnings.filterwarnings("ignore", category=UserWarning)
warnings.simplefilter("ignore", category=UserWarning, lineno=329, append=False)
warnings.filterwarnings('ignore', message='The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*',
                       category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)
parent_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, parent_dir)

sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from ..LP import *

def module_3(input_folder, output_folder, params):
    folder_py = params['folder_past_year']
    lang = ''
    counter = 0
    for file in os.listdir(input_folder):
        # Check if the file is an Excel file
        if file.endswith('.xlsx') or file.endswith('.xls') or file.endswith('.xlsm'):
            counter+=1
            errors = [], # Список ошибок
            
            print(f"Processing file {counter}: {file}")
            # Process the Excel file
            file_path = os.path.join(input_folder, file)

            # Language detection
            if 'Salary Data' in pd.ExcelFile(file_path).sheet_names:
                lang = 'ENG'
            else:
                lang = 'RUS'

            # Exporting the dataframe from an excel file
            # For SDFs
            sheet_name = "Total Data"
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            # print(df.keys())

            # Apply cleaning to column names
            df.columns = [re.sub(r'\s+', ' ', str(col).replace('\n', ' ').replace('\r', ' ')).strip() 
                            for col in df.columns]
            
            # leaving only required columns
            # df_old = df
            # df = pd.concat([df.iloc[:,:8], df.iloc[:, 22:25]])
            # print(df.columns)

            col_name = 'Название компании (заполняется автоматически)'

            unique_companies = df[col_name].dropna().unique()
            company_files = {}

            print(f"Компании в файле: {unique_companies}")

            wb = load_workbook(file_path)
            output_file = os.path.join(output_folder, "data_highlighted.xlsx")
            wb.save(output_file)

            # Проходим по каждой уникальной компании
            for company in unique_companies:
                print(f"Компания: {company}")
                # Проверяем участвовала ли компания в обзоре прошлых лет
                found_files = check_if_past_year_exist(company, folder_py)
                # Проверяем, проставлены ли коды
                codes = check_if_codes_exist(df, company)

                if codes and found_files:
                    print("Коды проставлены, есть файл с прошлого года")
                    process_with_past_year(company, found_files, df, output_file, folder_py)
                elif codes:
                    print("Коды проставлены, компания не участвовала в обзоре до этого")
                    fill_null_columns(output_file, df, company)
                elif found_files:
                    print("Коды не проставлены, есть файл с прошлого года")
                    # подтянуть коды с прошлого года
                else:
                    print("Коды не проставлены, компания не участвовала в обзоре до этого")
                    # проставить коды нейронкой

        print("-------------------------")        
        # if company_files:
        #     process_with_past_year(company_files, df)


def fill_null_columns(file, df, company):
    wb = load_workbook(file)
    ws = wb['Total Data']

    orange_fill = PatternFill(start_color="FFA500", end_color="FFA500", fill_type="solid")

    col_idx = list(df.columns).index(function_code) + 1

    for i in df.index[df[company_name] == company]:
        if pd.isna(df.at[i, function_code]):
            excel_row = i + 2  
            ws.cell(row=excel_row, column=col_idx).fill = orange_fill

    wb.save(file)
    print(f"Файл сохранён: {file}")


def process_with_past_year(company, company_files, df, file, folder):
    df_old = df
    df = df.loc[:, [company_name, dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                             job_title, function_code, subfunction_code, specialization_code]]
    df = df.loc[df[company_name] == company]
    file_to_cmp = os.path.join(folder, company_files[0])
    print("FILE: ", file_to_cmp)

    df_py = pd.read_excel(file_to_cmp, sheet_name=rem_data, header=6)
    df_py = df_py.loc[:, [company_name, dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                             job_title, function_code, subfunction_code, specialization_code]]
    df_py = df_py.loc[df_py[company_name] == company]

    # --- ---

    df_diff = df.merge(df_py, how='left', indicator=True)
    df_missing = df_diff.query('_merge == "left_only"').drop(columns=['_merge'])

    set_py = set(tuple(row) for row in df_py.itertuples(index=False, name=None))

    missing_indices = []
    for idx, row in df.iterrows():
        tup = tuple(row[c] for c in df.columns)
        if tup not in set_py:
            missing_indices.append(idx)

    print(f"Компания: {company}")
    print(f"Новых строк: {len(missing_indices)}")

    wb = load_workbook(file)
    ws = wb['Total Data'] 

    orange_fill = PatternFill(start_color="FFA500", end_color="FFA500", fill_type="solid")

    # Выделяем строки, которых нет в df_py
    for idx in missing_indices:
        excel_row = idx + 2
        for col_idx in range(1, len(df_old.columns) + 1):
            ws.cell(row=excel_row, column=col_idx).fill = orange_fill

    wb.save(file)
    print(f"Файл сохранён с выделенными строками: {file}")


def check_if_codes_exist(df, company):
    empty_count = df.loc[df[company_name]==company][function_code].isna().sum()
    non_empty_count = df.loc[df[company_name]==company][function_code].notna().sum()
    return empty_count <= non_empty_count

def check_if_past_year_exist(company, folder_py):
    company_str = str(company).strip()
    found_files = []
    
    for filename in os.listdir(folder_py):
        if company_str.lower() in filename.lower():
            found_files.append(filename)
    
    if found_files:
        # company_files[company] = found_files
        # print(f"Компания: {company_str}")
        for f in found_files:
            print(f"Найден файл: {f}")
        print()
    return found_files