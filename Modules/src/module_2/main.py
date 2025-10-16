
# All the variables are imported from LP.py file
import pandas as pd
from openpyxl import load_workbook
import openpyxl
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
from function_model.inference import predict_codes

def module_2(input_folder, output_folder, params):
    '''- codes & past_year_exist -> выделяем строки, в которых коды не совпадают
       - codes -> выделяем строки, в которых кодов нет
       - past_yesr_exist -> подтягиваем коды с прошлого года и выделяем пустые (либо проставляем нейронкой)
       - nothing -> проставляем все коды нейронкой'''

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
                    file_to_cmp = os.path.join(folder_py, found_files[0])
                    df_py = pd.read_excel(file_to_cmp, sheet_name=rem_data, header=6)
                    df_py = df_py.loc[df_py[company_name] == company]
                    cols = [company_name, dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                             job_title]
                    cols_to_copy = [function_code, subfunction_code, specialization_code, function, subfunction, specialization]
                    copy_columns_from_py_preserve_excel(df, df_py, cols, cols_to_copy, excel_path=output_file)
                else:
                    print("Коды не проставлены, компания не участвовала в обзоре до этого")
                    predict_codes_by_model(df, output_file, company)
                    # проставить коды нейронкой
                print("\n########################")

        print("-------------------------")
        

def predict_codes_by_model(df, output_file, company):
    df = df.loc[df[company_name] == company]
    preds = predict_codes(df)
    

def _normalize_val(v):
    """Нормализация для сравнения: на str, strip и lower (None/NaN -> '')"""
    if pd.isna(v):
        return ""
    s = str(v).strip()
    return s.lower()

def copy_columns_from_py_preserve_excel(df, df_py, cols, cols_to_copy,
                                       excel_path,
                                       sheet_name="Total Data",
                                       overwrite_nonempty=False,
                                       normalize=True):
    """
    Сравнивает строки по ключевым колонкам `cols` (по значениям) и копирует из df_py в Excel
    значения колонок cols_to_copy там, где найдено совпадение. Не трогает форматирование книги.
    
    Параметры:
    - df: pd.DataFrame с текущими строками (используется для нормализации/ключей).
    - df_py: pd.DataFrame источник (прошлогодний).
    - cols: list, ключевые колонки для сопоставления.
    - cols_to_copy: list, колонки, которые копируем из df_py в Excel.
    - excel_path: путь к существующему Excel файлу (если нет — будет создан).
    - sheet_name: имя листа в Excel (по умолчанию 'Total Data'); если нет — будет взят первый лист.
    - overwrite_nonempty: если True — перезаписывает непустые ячейки; False — только пустые.
    - normalize: если True — сравнение регистронезависимое и без крайних пробелов.
    """

    def key_from_row(row):
        vals = [row.get(c, "") for c in cols]
        return tuple(_normalize_val(v) if normalize else ("" if pd.isna(v) else v) for v in vals)

    mapping = {}
    for _, r in df_py.iterrows():
        k = key_from_row(r)
        mapping[k] = {c: r.get(c, None) for c in cols_to_copy if c in df_py.columns}

    try:
        wb = openpyxl.load_workbook(excel_path)
        created = False
    except FileNotFoundError:
        print('File not found')
        return

    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    else:
        if created:
            ws = wb.active
            ws.title = sheet_name
        else:
            ws = wb[wb.sheetnames[0]]

    # Сопоставление заголовков столбцов в excel
    header_row = 1
    excel_headers = {}
    max_col = ws.max_column
    for col_idx in range(1, max_col + 1):
        cell = ws.cell(row=header_row, column=col_idx)
        if cell.value is not None:
            excel_headers[str(cell.value).strip()] = col_idx

    # Проходим по строкам и ищем совпадения
    first_data_row = header_row + 1
    rows_scanned = 0
    rows_updated = 0

    for r in range(first_data_row, ws.max_row + 1):
        rows_scanned += 1
        key_vals = []
        for kcol in cols:
            col_idx = excel_headers[kcol]
            val = ws.cell(row=r, column=col_idx).value
            key_vals.append(_normalize_val(val) if normalize else ("" if val is None else val))
        key = tuple(key_vals)

        if key in mapping:
            updates = mapping[key]
            did_update = False
            for col_name, new_val in updates.items():
                col_idx = excel_headers[col_name]
                cell = ws.cell(row=r, column=col_idx)
                cell_current = cell.value
                is_empty = (cell_current is None) or (str(cell_current).strip() == "")
                if is_empty or overwrite_nonempty:
                    cell.value = new_val
                    did_update = True
            if did_update:
                rows_updated += 1

    wb.save(excel_path)
    print(f"Scanned {rows_scanned} rows on sheet '{ws.title}'. Updated {rows_updated} rows in '{excel_path}'.")


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

    # print(f"Компания: {company}")
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
        # print()
    return found_files