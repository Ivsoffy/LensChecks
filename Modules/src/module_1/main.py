
# All the variables are imported from LP.py file
import pandas as pd
import numpy as np
from datetime import datetime, date
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side, numbers
import os
import sys
import warnings
import re
import difflib
import time
import shutil
import warnings
import os
import win32com.client
import shutil, os
from win32com.client import makepy

# warnings.filterwarnings("ignore", category=UserWarning)
warnings.simplefilter("ignore", category=UserWarning, lineno=329, append=False)
warnings.filterwarnings('ignore', message='The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*',
                       category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)
parent_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, parent_dir)

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from ..LP import *
from ..utils import main_checks, write_df_with_template, is_empty_value


def get_valid_path(prompt):
    while True:
        path = input(prompt)
        path = path.replace("\\", "/")  # Replace backslashes with forward slashes
        if os.path.isdir(path):
            return path
        else:
            print("Invalid path. Please try again.")

def check_general_info(errors, df_company, lang, df):
    # Setting columns names to the russian version
    df.columns = expected_columns_rus

    try: #добавить проверку на выпадающий список
        df[company_name] = df_company.iloc[0, 1]
        df[gi_company_name] = df_company.iloc[0, 1]
        df[gi_sector] = df_company.iloc[1, 1]
        df[gi_origin] = df_company.iloc[2, 1]
        df[gi_headcount_cat] = df_company.iloc[3, 1]
        df[gi_revenue_cat] = df_company.iloc[4, 1]
        df[gi_contact_name] = df_company.iloc[5, 1]
        df[gi_title] = df_company.iloc[6, 1]
        df[gi_tel] = df_company.iloc[7, 1]
        df[gi_email] = df_company.iloc[8, 1]
    except Exception as e:
        print(e)

    for _, row in df_company.loc[0:3, ["Unnamed: 2", "Unnamed: 3"]].iterrows():
        field_name = str(row["Unnamed: 2"]).strip()   # Название поля
        value = row["Unnamed: 3"]                     # Значение
        if is_empty_value(value):
            errors['info_errors'].append(f"Incorrect General Info: {field_name}")
        

    comp_name = df[company_name][0]
    if not re.fullmatch(r"[A-Za-z0-9_]+", str(comp_name)):
        errors['info_errors'] += [f"Incorrect company name format: {comp_name}"]

    df['SDF Language'] = lang
    return errors, df 
          
def region_normalization(errors, text: str, index: int, lang) -> str:
    not_missing = not pd.isna(text)
    if lang == 'RUS':
        region_values = list(set(final_region.values()))
    else:
        region_values = list(set(final_region_eng.values()))

    normalized_to_original = {value.strip().lower(): value for value in region_values}
    normalized_text = str(text).strip().lower() if not_missing else ""

    matched_text = normalized_to_original.get(normalized_text)
    if not matched_text and not_missing:
        closest_match = difflib.get_close_matches(
            normalized_text,
            normalized_to_original.keys(),
            n=1,
            cutoff=0.75
        )
        if closest_match:
            matched_text = normalized_to_original[closest_match[0]]

    in_dict_values = matched_text is not None
    if in_dict_values:
        text = matched_text
    if not(not_missing and in_dict_values):
        errors['data_errors'] += [(region, index)]
    return text

def convert_some_columns_to_numeric(df):
    # Defining columns where ',' will be replaced with '.' so that it is recognized as a number
    columns_to_numeric = [monthly_salary, salary_rate, number_monthly_salaries, fact_sti, fact_lti, fact_lti_1, fact_lti_2, fact_lti_3, target_lti_per, additional_pay, grade]
    
    for column in columns_to_numeric:
        df[column] = df[column].astype(str).str.replace(',', '.').str.replace(u'\xa0', '')
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = df[column].replace('nan', np.nan)
    return df

def convert_some_columns_to_str(df):
    columns_to_str = [man_emp, gender_id, sti_eligibility, lti_eligibility, expat, performance, function_code, subfunction_code, specialization_code]
    for column in columns_to_str:
        df[column] = df[column].astype(str)
    return df

# Function to assign values based on a mapping
def translate_values(df, columns, translation_map):
    """
    Translate values in specified DataFrame column(s) using a provided mapping dictionary.
    
    Parameters:
    df: pandas DataFrame
    columns: str or list of str, column name(s) to translate
    translation_map: dict, mapping of original values to translated values
    
    Returns:
    pandas DataFrame with translated values
    """
    # Create a copy to avoid modifying the original DataFrame
    df_copy = df.copy()
    
    # Ensure columns is a list for uniform processing
    if isinstance(columns, str):
        columns = [columns]
    
    # Apply translation to each specified column
    for col in columns:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].map(translation_map).fillna(df_copy[col])
        else:
            print(f"Warning: Column '{col}' not found in DataFrame")
    
    return df_copy

def map_column_values(df, check_column, amend_column, mapping_dict):
    """
    Check values in one column and assign mapped values to another column.
    
    Parameters:
    df: pandas DataFrame
    check_column: str, name of the column to check values in
    amend_column: str, name of the column to assign mapped values to
    mapping_dict: dict, mapping of check_column values to amend_column values
    
    Returns:
    pandas DataFrame with amended values
    """
    # Create a copy to avoid modifying the original DataFrame
    df_copy = df.copy()
    
    # Check if both columns exist
    if check_column not in df_copy.columns:
        print(f"Warning: Check column '{check_column}' not found in DataFrame")
        return df_copy
    
    if amend_column not in df_copy.columns:
        print(f"Warning: Amend column '{amend_column}' not found in DataFrame")
        return df_copy
    
    # Map values from check_column to amend_column
    df_copy[amend_column] = df_copy[check_column].map(mapping_dict).fillna(df_copy[amend_column])
    
    return df_copy

def eng_to_rus(df):
    # Apply translations using the tranlsation function | Converting English version to Russian
    df = translate_values(df, [expat, sti_eligibility, lti_eligibility], yes_no_map)
    df = translate_values(df, man_emp, manager_spec_map)
    df = translate_values(df, performance, performance_map)
    df = translate_values(df, gender_id, gender_map)
    df = translate_values(df, region, region_match_map)
    df = translate_values(df, tenure, tenure_map)
    df = translate_values(df, [lti_prog_1, lti_prog_2, lti_prog_3], lti_map)
    df = translate_values(df, gi_sector, sector_map)
    df = translate_values(df, gi_origin, origin_map)
    df = translate_values(df, gi_revenue_cat, revenue_map)

    return df

def add_errors_to_excel(errors, input_path, output_path):
    """Добавляет лист 'Errors' и подсвечивает ячейки с ошибками с использованием win32com."""
    try:
        excel = win32com.client.Dispatch("Excel.Application")
    except AttributeError:
        # Если кэш повреждён или не создан — чистим и пробуем снова
        gen_py = os.path.join(os.path.expanduser("~"), "AppData\\Local\\Temp\\gen_py")
        shutil.rmtree(gen_py, ignore_errors=True)
        makepy.main(["Microsoft Excel * Object Library"])
        excel = win32com.client.Dispatch("Excel.Application")


    input_path = os.path.abspath(input_path)
    output_path = os.path.abspath(output_path)
    # --- Формирование таблицы ошибок ---
    info = errors.get('info_errors', [])
    data = [col for col, _ in errors.get('data_errors', [])]
    unique_data = list(dict.fromkeys(data))
    n = max(len(info), len(unique_data))
    df_errors = pd.DataFrame({
        'info_errors': info + [None] * (n - len(info)),
        'data_errors': unique_data + [None] * (n - len(unique_data))
    })

    # --- Открытие Excel ---
    # excel = win32com.client.Dispatch("Excel.Application")
    excel.Visible = False
    excel.DisplayAlerts = False
    wb = excel.Workbooks.Open(input_path)

    # --- Проверка и создание листа Errors ---
    for sheet in wb.Sheets:
        if sheet.Name.lower().strip() == "errors":
            sheet.Delete()
    ws_err = wb.Sheets.Add(Before=wb.Sheets(1))
    ws_err.Name = "Errors"

    # --- Запись таблицы ошибок ---
    ws_err.Cells(1, 1).Value = "info_errors"
    ws_err.Cells(1, 2).Value = "data_errors"
    for r in range(len(df_errors)):
        ws_err.Cells(r + 2, 1).Value = df_errors.iloc[r, 0]
        ws_err.Cells(r + 2, 2).Value = df_errors.iloc[r, 1]

    # --- Форматирование заголовка ---
    header_range = ws_err.Range(ws_err.Cells(1, 1), ws_err.Cells(1, 2))
    header_range.Font.Bold = True
    header_range.Font.Color = 0xFFFFFF      # Белый
    header_range.Interior.Color = 0x4472C4  # Синий (BGR)
    header_range.HorizontalAlignment = -4108  # xlCenter
    header_range.Borders.Weight = 2

    # --- Форматирование тела таблицы ---
    used_range = ws_err.UsedRange
    used_range.Columns.AutoFit()
    for row in ws_err.Range(ws_err.Cells(2, 1), ws_err.Cells(df_errors.shape[0] + 1, 2)):
        row.Borders.Weight = 2
        row.WrapText = True
        row.VerticalAlignment = -4160  # xlTop

    # --- Определение листа "Данные" ---
    data_sheet = None
    for s in wb.Sheets:
        name = s.Name.strip().lower()
        if name in ("данные", "salary data"):
            data_sheet = s
            break
    if not data_sheet:
        wb.Close(SaveChanges=False)
        excel.Quit()
        raise ValueError("Не найден лист 'Данные'.")

    ws_data = data_sheet

    # --- Определяем начало данных ---
    df_head = pd.read_excel(input_path, sheet_name=ws_data.Name, header=None, nrows=40)
    non_empty = df_head.notna().sum(axis=1)
    header_end = max((i for i, v in enumerate(non_empty) if v >= max(3, df_head.shape[1] * 0.05)), default=0)

    def norm(s):
        return str(s).strip().lower() if pd.notna(s) else ""

    col_map = {norm(df_head.iat[r, c]): c + 1
               for r in range(header_end + 1)
               for c in range(df_head.shape[1])
               if pd.notna(df_head.iat[r, c])}

    # --- Подсветка ошибок ---
    orange_color = 0x00C0FF  # BGR = (0xFF, 0xC0, 0x00) → оранжевый
    for col_name, idx in errors.get('data_errors', []):
        col_idx = col_map.get(norm(col_name))
        if not col_idx:
            print(f"Не найдена колонка: {col_name}")
            continue
        excel_row = 8 + idx
        if 1 <= excel_row <= ws_data.UsedRange.Rows.Count:
            ws_data.Cells(excel_row, col_idx).Interior.Color = orange_color
        else:
            print(f"Строка вне диапазона: {excel_row}")

    # --- Сохранение и закрытие ---
    wb.SaveAs(output_path)
    wb.Close(SaveChanges=True)
    excel.Quit()

    print(f"Лист 'Errors' добавлен, ячейки подсвечены. Файл: {output_path}")

def add_regions(errors, df, lang):
    df[region] = df[region].where(
        ~df[region].isna() & (df[region].astype(str).str.strip() != ''),
        df[region_client_fill]
    )
    df[region] = df[region].astype(str).str.lower()
    df = translate_values(df, region, final_region)
    if lang == 'RUS':
        df = translate_values(df, region, final_region)
    else:
        df = translate_values(df, region, final_region_eng)
    df[region] = df.apply(lambda x: region_normalization(errors, x[region_client_fill], x.name, lang), axis=1)

    df[macroregion] = np.nan
    df = map_column_values(df, region, macroregion, region_to_macroregion_map)
    return errors, df

def errors_rus_to_eng(errors):
    for ind, error in enumerate(errors['data_errors']):
        new_error_ind = expected_columns_rus.index(error[0])
        new_error = (expected_columns_eng[new_error_ind], error[1])
        errors['data_errors'][ind] = new_error
    return errors

def check_and_process_data(errors, df, lang, params):

    df = convert_some_columns_to_numeric(df)
    df = convert_some_columns_to_str(df)

    if lang == 'ENG':
        df = eng_to_rus(df)

    errors, df = add_regions(errors, df, lang)
    errors, df = main_checks(errors, df)

    if lang == 'ENG':
        errors = errors_rus_to_eng(errors)
    
    return errors, df

def module_1(input_folder, output_folder, params=None):

    print("Модуль 1: Техническая проверка.")

    # Additional columns from General Info sheet from the SDFs
    additional_cols = [gi_sector, gi_origin, gi_headcount_cat, gi_revenue_cat, gi_contact_name, 
                    gi_title, gi_tel, gi_email, 'SDF Language']

    expected_columns = expected_columns_rus

    # Setting the columns in the final df
    final_cols = expected_columns + additional_cols

    # Creating the final df
    # Iterate through all the files in the input folder
    process_start = time.time()

    unprocessed_files = file_processing(input_folder, output_folder, final_cols, params)
    proces_end = time.time()
    print(f'Обработка файлов заняла: {proces_end - process_start}')

    if len(unprocessed_files) == 0:
        print(f"\nВсе файлы проверены!")
    else:
        print("=" * 20 + " WARNING! " + "=" * 20)
        print(f"List of unprocessed files:")
        for file, issue in unprocessed_files.items():
            data_err = [col for col, _ in issue.get('data_errors', [])]
            unique_data_err = list(dict.fromkeys(data_err))
            print(f'\n')
            print(f"File: {file}, Info errors: {issue['info_errors']}\nData errors: {unique_data_err}")
            
            
    # Create unprocessed folder if it doesn't exist
    unprocessed_folder = os.path.join(output_folder, 'unprocessed')
    os.makedirs(unprocessed_folder, exist_ok=True)

    # Copy unprocessed files to the unprocessed folder (overwrite if exists)
    if unprocessed_files:
        print(f"\nСохраняем {len(unprocessed_files)} файлов в 'unprocessed'...")

        for file_name, issue in unprocessed_files.items():
            source_path = os.path.join(output_folder, file_name)
            destination_path = os.path.join(unprocessed_folder, file_name)
            # print(source_path)
            # print(destination_path)
            try:
                if os.path.exists(source_path):
                    # Если файл уже есть в папке unprocessed — удалим его
                    if os.path.exists(destination_path):
                        os.remove(destination_path)
                    add_errors_to_excel(issue, source_path, destination_path)
                    os.remove(source_path)
            except Exception as e:
                print(f"Не удалось сохранить файл {file_name} в unprocessed: {str(e)}")
        
def file_processing(input_folder, output_folder, columns, params):
    # Creating a list for files with issues
    unprocessed_files = {}
    single_db = params['single_db']
    result_df = pd.DataFrame()
    # ultimate_df = pd.DataFrame(columns=columns)
    counter = 0
    save_db_only_without_errors = params['save_db_only_without_errors']

    for file in os.listdir(input_folder):
        # Check if the file is an Excel file
        if file.endswith('.xlsx') or file.endswith('.xls') or file.endswith('.xlsm'):
            counter += 1
            errors = {
                'info_errors': [], # Список ошибок на листе Общая информация
                'data_errors': [] # Cписок ошибок в данных (row, col)
            }
            
            print(f"Проверяем файл {counter}: {file}")
            # Process the Excel file
            file_path = os.path.join(input_folder, file)

            # Language detection
            if 'Salary Data' in pd.ExcelFile(file_path).sheet_names:
                lang = 'ENG'
                rm_data = rem_data_eng 
                cmp_data = company_data_eng
                rows_to_drop = [company_name_eng, job_title_eng]
            else:
                lang = 'RUS'
                rm_data = rem_data
                cmp_data = company_data
                rows_to_drop = [company_name, job_title]
            expected_columns = set_expected_columns(lang)

            # Exporting the dataframe from an excel file
            # For SDFs
            df = pd.read_excel(file_path, sheet_name=rm_data, header=6)

            # Apply cleaning to column names
            df.columns = [re.sub(r'\s+', ' ', str(col).replace('\n', ' ').replace('\r', ' ')).strip() 
                            for col in df.columns]
            
            # Check if all expected columns are present
            missing_columns_rem_data = [col for col in expected_columns if col not in df.columns]

            if missing_columns_rem_data:
                errors['info_errors'] += [f"Не хватает следующих колонок в Данных: {missing_columns_rem_data}"]
            else:
                # leaving only required columns
                df = df[expected_columns]
            
                # Cleaning all the blanks from the columns
                for column in rows_to_drop:
                    df[column] = df[column].replace('', np.nan)

                # Dropping rows where company name and title are empty at the same time
                df.dropna(subset=rows_to_drop, how = 'all', inplace=True)

                df_company = pd.read_excel(file_path, sheet_name=cmp_data, header=1)
                df_company = df_company.iloc[:, 2:]


                # Taking the data from the General Info sheet
                errors, df = check_general_info(errors, df_company, lang, df)
                errors, df = check_and_process_data(errors, df, lang, params)

                if single_db:
                    result_df = pd.concat([result_df, df])
                # print(df.shape[0])
                
                if not single_db and ((errors['data_errors'] == [] and errors['info_errors'] == []) or not save_db_only_without_errors):
                    # Save the processed DataFrame to the output folder
                    file_output_path_no_format = os.path.join(output_folder, file)
                    df.to_excel(file_output_path_no_format, sheet_name="Total Data")
                    print(f"Анкета {file} сохранена в {output_folder}!")
            if errors['data_errors'] != [] or errors['info_errors'] != []:
                base, ext = os.path.splitext(file)
                file_output_path = os.path.join(output_folder, f'{base}_unprocessed_{ext}')
                write_df_with_template(df, file_path, file_output_path)
                unprocessed_files[os.path.basename(file_output_path)] = errors
            else:
                print("В файле не обнаружено ошибок, мои поздравления!")
    
    if single_db and ((not save_db_only_without_errors) or (errors['data_errors'] == [] and errors['info_errors'] == [])):
        file_output_path = os.path.join(output_folder, 'result_db.xlsx')
        result_df = result_df.loc[:, ~df.columns.str.contains('^Unnamed')]
        result_df.to_excel(file_output_path, sheet_name='Total Data')
        print(f"Все анкеты объединены в {output_folder}!")

    return unprocessed_files
