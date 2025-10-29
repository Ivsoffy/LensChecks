
# All the variables are imported from LP.py file
import pandas as pd
import numpy as np
from datetime import datetime
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

# print('Calculating compensation elements and running checks on data\nProperty of Lens Consulting & Lens Digital\nRelease version: 1.0\n\n')

# Function to enter the file paths
def get_valid_path(prompt):
    while True:
        path = input(prompt)
        path = path.replace("\\", "/")  # Replace backslashes with forward slashes
        if os.path.isdir(path):
            return path
        else:
            print("Invalid path. Please try again.")


# ### Функции для проверок
def man_emp_normalization(text: str, index) -> str:
    global errors

    text = text.lower().strip()

    if not text or text == 'nan' or text == '':
        # errors['data_errors'] += [(man_emp, index)]
        return text

    managers = ["руководитель", "руководители", "менеджер", "менеджеры", "manager", "managers"]
    specialists = ["рабочий", "рабочие", "служащий", "служащие", "специалист", "специалисты", "specialist", "specialists"]

    all_keywords = managers + specialists
    words = re.findall(r"\w+", text)

    for word in words:
        match = difflib.get_close_matches(word, all_keywords, n=1, cutoff=0.7)
        if match:
            if match[0] in managers:
                return "Руководитель"
            elif match[0] in specialists:
                return "Специалист"

    errors['data_errors'] += [(man_emp, index)]
    return text


def expectation_normalization(text: str, index: int) -> str:
    global errors
    valid = ["Соответствует ожиданиям", "Ниже ожиданий", "Выше ожиданий"]
    valid_eng = ['Meet expectations', 'Below expectations', 'Above expectations']

    if not text or text.strip() == '' or text == 'nan':
        return '-'

    text = text.strip().lower()
    match = difflib.get_close_matches(text, [v.lower() for v in valid], n=1, cutoff=0.6)
    match_eng = difflib.get_close_matches(text, [v.lower() for v in valid_eng], n=1, cutoff=0.6)

    if match:
        for v in valid:
            if v.lower() == match[0]:
                return v
    elif match_eng:
        for ind in range(len(valid_eng)):
            if valid_eng[ind].lower() == match_eng[0]:
                return valid[ind]
    else:
        # errors['data_errors'] += [(performance, index)]
        # return text
        return '-'


def level_normalization(value, index) -> str:
    """
    Преобразует значение в формат 'N-X' (где X от 1 до 20)
    """
    global errors
    if value is not None:
        text = str(value).strip().upper()
        # Число из строки вроде 'N-3', 'n3', '3'
        match = re.search(r'(\d{1,2})', text)
        if match:
            num = int(match.group(1))
            if 1 <= num <= 20:
                return f"N-{num}"
    
    # errors['data_errors'] += [(n_level, index)]
    return '-'


def number_monthly_salaries_normalization(num, index):
    global errors

    if pd.isna(num) or num == '':
        num = 12
    elif num < 12 or num > 15:
        errors['data_errors'] += [(number_monthly_salaries, index)]
    return num


def gender_normalization(text: str, index: int) -> str:
    global errors

    if text == '' or text == 'nan':
        # errors['data_errors'] += [(gender_id, index)]
        return '-'

    text = text.lower().strip()

    woman = ["female", "женский", "жен", "f", "ж-й", 'ж', 'женщина']
    man = ["male", "мужской", "муж", "m", "м-й", 'м', 'мужчина']

    all_keywords = woman + man
    words = re.findall(r"\w+", text)

    for word in words:
        match = difflib.get_close_matches(word, all_keywords, n=1, cutoff=0.7)
        if match:
            if match[0] in woman:
                return "Ж"
            elif match[0] in man:
                return "М"

    # errors['data_errors'] += [(gender_id, index)]
    return '-'
            

def region_normalization(text: str, index: int, lang) -> str:
    global errors

    not_missing = not pd.isna(text)
    if lang == 'RUS':
        in_dict_values = text in (set(final_region.values()))
        # print("rus", in_dict_values, " ", text)
    else:
        in_dict_values = text in (set(final_region_eng.values()))
    
    if not(not_missing and in_dict_values):
        errors['data_errors'] += [(region, index)]

    return text


def convert_some_columns_to_numeric(df):
    # Defining columns where ',' will be replaced with '.' so that it is recognized as a number
    columns_to_numeric = [monthly_salary, salary_rate, number_monthly_salaries, fact_sti, fact_lti, target_lti_per, additional_pay]
    
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

def salary_rate_normalization(num: int, index: int) -> str:
    global errors

    if not num or pd.isna(num):
        num = 1
    elif num >= 1.5 or num <= 0:
            errors['data_errors'] += [(salary_rate, index)]
    return num

def additional_pay_normalization(value, index):
    global errors

    if pd.isna(value):
        if region in regions_with_surcharges:
            errors['data_errors'] += [(additional_pay, index)]
    elif value < 0:
        errors['data_errors'] += [(additional_pay, index)]

    return value


def eligibility_normalization(fact, target, value, index):
    if not pd.isna(value):
        value = value.strip().lower()

        if value in ['да', 'д', 'yes', 'y']:
            return "Да"
        else:
            return "Нет"

    else:
        if not(pd.isna(fact) or target=='nan'):
            return "Да"
        else:
            return "Нет"

def fact_sti_normalization(eligibility, value, index):
    global errors
    if eligibility == 'Нет' and not pd.isna(value):
        # print(f"value: {value}, eligibility: {eligibility}")
        errors['data_errors'] += [(fact_sti, index)]
    return value

# Проверка листа "Общая информация"
def check_general_info(df_company, lang, df):
    global errors
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
    except (IndexError, KeyError):
        errors['info_errors'] += ["Incorrect General Info"]

    # print(x for x in df)
    if any(x is None or str(x).strip() == "" for x in df):
        errors['info_errors'] += ["Incorrect General Info"]
    else:
        comp_name = df[company_name][0]
        if not re.fullmatch(r"[A-Za-z_]+", str(comp_name)):
            errors['info_errors'] += [f"Incorrect company name format: {comp_name}"]
            # print(df[gi_company_name

    df['SDF Language'] = lang
    return df 

def target_sti_normalization(text: str, index: int) -> str:
    global errors
    # Оставлять ли проценты
    return text
    

def lti_checks(main_lti, lti_1, lti_2, lti_3, index, type_lti):
    global errors
    if not ((main_lti == (lti_1 + lti_2 + lti_3)) | np.isnan(main_lti)):
        errors['data_errors'] += [(type_lti, index)]
    return main_lti

def add_errors_to_excel(errors, input_path, output_path):
    """Добавляет лист 'Ошибки' и подсвечивает ячейки с ошибками на листе 'Данные'."""
    # --- Формирование таблицы ошибок ---
    info = errors.get('info_errors', [])
    data = [col for col, _ in errors.get('data_errors', [])]
    unique_data = list(dict.fromkeys(data))
    n = max(len(info), len(unique_data))
    df_errors = pd.DataFrame({
        'info_errors': info + [None] * (n - len(info)),
        'data_errors': unique_data + [None] * (n - len(unique_data))
    })

    wb = load_workbook(input_path, data_only=True)
    ws_err = wb.create_sheet("Errors", 0)

    # --- Запись и оформление листа "Ошибки" ---
    for r, row in enumerate(dataframe_to_rows(df_errors, index=False, header=True), 1):
        for c, v in enumerate(row, 1):
            ws_err.cell(r, c, v)

    header_style = {"font": Font(bold=True, color="FFFFFF"),
                    "fill": PatternFill("solid", fgColor="4472C4")}
    border = Border(*[Side(style="thin", color="808080")]*4)
    for cell in ws_err[1]:
        cell.font, cell.fill = header_style["font"], header_style["fill"]
        cell.alignment, cell.border = Alignment(horizontal='center'), border
    for row in ws_err.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            cell.border = border
    for col in ws_err.columns:
        ws_err.column_dimensions[col[0].column_letter].width = max(len(str(c.value) or "") for c in col) + 2

    # --- Определение структуры листа "Данные" ---
    data_sheet = next((s for s in wb.sheetnames if (s.strip().lower() == "данные") or s.strip().lower() == "salary data"), None)
    if not data_sheet:
        raise ValueError("Не найден лист 'Данные'.")
    ws_data = wb[data_sheet]
    df_head = pd.read_excel(input_path, sheet_name=data_sheet, header=None, nrows=40)

    non_empty = df_head.notna().sum(axis=1)
    header_end = max((i for i, v in enumerate(non_empty) if v >= max(3, df_head.shape[1] * 0.05)), default=0)
    data_start = header_end + 2

    def norm(s): return str(s).strip().lower() if pd.notna(s) else ""
    col_map = {norm(df_head.iat[r, c]): c + 1
               for r in range(header_end + 1) for c in range(df_head.shape[1])
               if pd.notna(df_head.iat[r, c])}

    # --- Подсветка ошибок ---
    orange = PatternFill("solid", fgColor="FFC000")
    for col_name, idx in errors.get('data_errors', []):
        col_idx = col_map.get(norm(col_name))
        if not col_idx:
            print(f"Не найдена колонка: {col_name}")
            continue
        excel_row = 8 + idx
        if 1 <= excel_row <= ws_data.max_row:
            ws_data.cell(excel_row, col_idx).fill = orange
        else:
            print(f"Строка вне диапазона: {excel_row}")

    wb.save(output_path)
    print(f"Лист 'Ошибки' добавлен, ячейки подсвечены. Файл: {output_path}")

def monthly_salary_normalization(row, index):
    global errors
    if pd.isna(row):
        errors['data_errors'] += [(monthly_salary, index)]
    return row

def errors_rus_to_eng(errors):
    for ind, error in enumerate(errors['data_errors']):
        new_error_ind = expected_columns_rus.index(error[0])
        new_error = (expected_columns_eng[new_error_ind], error[1])
        errors['data_errors'][ind] = new_error
    return errors
    # находим индекс ошибки в expected_columns_rus, меняем на колонку-ошибку из expected_columns_eng


# ### Проверка 
def check_and_process_data(df, lang, params):
    global errors

    df = convert_some_columns_to_numeric(df)
    df = convert_some_columns_to_str(df)
    if lang == 'ENG':
        df = eng_to_rus(df)

    drop_empty_month_salary = params['drop_empty_month_salary']
    
    # Название должности
    df[job_title] = df.apply(lambda x: '-' if (not x[job_title]) or (str(x[job_title]).strip() == 'nan') or (str(x[job_title]).strip() == '') else x[job_title], axis=1)
    # Руководитель/специалист
    df[man_emp] = df.apply(lambda x: man_emp_normalization(x[man_emp], x.name), axis=1)
    # Оценка эффективности работы сотрудника
    df[performance] = df.apply(lambda x: expectation_normalization(x[performance], x.name), axis=1)
    # Уровень подчинения по отношению к Первому лицу компании
    df[n_level] = df.apply(lambda x: level_normalization(x[n_level], x.name), axis=1)
    # Пол
    df[gender_id] = df.apply(lambda x: gender_normalization(x[gender_id], x.name), axis=1)
    # Регион/область (заполняется автоматически)
    df[region] = df[region].astype(str).str.lower()
    if lang == 'RUS':
        df = translate_values(df, region, final_region)
    else:
        df = translate_values(df, region, final_region_eng)
    df[macroregion] = np.nan
    df = map_column_values(df, region, macroregion, region_to_macroregion_map)
    df[region] = df.apply(lambda x: region_normalization(x[region], x.name, lang), axis=1)
    # Размер ставки
    df[salary_rate] = df.apply(lambda x: salary_rate_normalization(x[salary_rate], x.name), axis=1)
    # Ежемесячный оклад
    if drop_empty_month_salary:
        df[monthly_salary] = df.dropna(subset=[monthly_salary], inplace=True)
    else:
        df[monthly_salary] = df.apply(lambda x: monthly_salary_normalization(x[monthly_salary], x.name), axis=1)
    # Число окладов в году
    df[number_monthly_salaries] = df.apply(lambda x: number_monthly_salaries_normalization(x[number_monthly_salaries], x.name), axis=1)
    # Постоянные надбавки и доплаты (общая сумма за год)
    df[additional_pay] = df.apply(lambda x: additional_pay_normalization(x[additional_pay], x.name), axis=1)
    # Право на получение переменного вознаграждения
    df[sti_eligibility] = df.apply(lambda x: eligibility_normalization(x[fact_sti], x[target_sti], x[sti_eligibility], x.name), axis=1)
    # Фактическая премия
    df[fact_sti] = df.apply(lambda x: fact_sti_normalization(x[sti_eligibility], x[fact_sti], x.name), axis=1)
    # Целевая премия (%)
    df[target_sti] = df.apply(lambda x: target_sti_normalization(x[target_sti], x.name), axis=1)
    # Фактическая стоимость всех предоставленных типов LTI за 1 год (AK)
    df[fact_lti] = df.apply(lambda x: lti_checks(x[fact_lti], x[fact_lti_1], x[fact_lti_2], x[fact_lti_3], x.name, fact_lti), axis=1)
    # Целевая стоимость всех предоставленных типов LTI в % от базового оклада за 1 год
    df[target_lti_per] = df.apply(lambda x: lti_checks(x[target_lti_per], x[target_lti_1], x[target_lti_2], x[target_lti_3], x.name, target_lti_per), axis=1)
    # Целевая стоимость вознаграждения как % от базового оклада [Данные] AO, AS, AW
    if lang == 'ENG':
        errors = errors_rus_to_eng(errors)
    return df

# Проверка каждого файла на наличие всех нужных колонок. При любой ошибке файл попадает в unprocessed.
def module_1(input_folder='companies/rus', output_folder='output', params=None):

    # Additional columns from General Info sheet from the SDFs
    additional_cols = [gi_sector, gi_origin, gi_headcount_cat, gi_revenue_cat, gi_contact_name, 
                    gi_title, gi_tel, gi_email, 'SDF Language']

    expected_columns = expected_columns_rus

    # Setting the columns in the final df
    final_cols = expected_columns + additional_cols

    # Creating the final df
    # Iterate through all the files in the input folder
    process_start = time.time()
    # print("Current working directory:", os.getcwd())
    unprocessed_files = file_processing(input_folder, output_folder, final_cols, params)
    proces_end = time.time()
    print(f'Обработка файлов заняла: {proces_end - process_start}')

    if len(unprocessed_files) == 0:
        print(f"\nВсе файлы проверены!")
    else:
        for file, issue in unprocessed_files.items():
            data_err = [col for col, _ in issue.get('data_errors', [])]
            unique_data_err = list(dict.fromkeys(data_err))
            # error_df = pd.DataFrame(data=issue)
            print(f'\n')
            print("=" * 20 + " WARNING! " + "=" * 20)
            print(f"List of unprocessed files:")
            print(f"File: {file}, Info errors: {issue['info_errors']}\nData errors: {unique_data_err}")
            
            
    # Create unprocessed folder if it doesn't exist
    unprocessed_folder = os.path.join(input_folder, 'unprocessed')
    os.makedirs(unprocessed_folder, exist_ok=True)

    # Copy unprocessed files to the unprocessed folder (overwrite if exists)
    if unprocessed_files:
        print(f"\nСохраняем {len(unprocessed_files)} файлов в 'unprocessed'...")

        for file_name, issue in unprocessed_files.items():
            source_path = os.path.join(input_folder, file_name)
            destination_path = os.path.join(unprocessed_folder, file_name)
            try:
                if os.path.exists(source_path):
                    # Если файл уже есть в папке unprocessed — удалим его
                    if os.path.exists(destination_path):
                        os.remove(destination_path)
                    add_errors_to_excel(issue, source_path, destination_path)
                    # shutil.copy2(source_path, destination_path)
                    # print(f"Copied: {file_name}")
            except Exception as e:
                print(f"Не удалось сохранить файл {file_name} в unprocessed: {str(e)}")

def file_processing(input_folder, output_folder, columns, params):
    global errors
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
                'info_errors': [], # Список ошибок
                'data_errors': [] # Cписок (row, col)
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
            df = check_general_info(df_company, lang, df)
            df = check_and_process_data(df, lang, params)

            if single_db:
                result_df = pd.concat([result_df, df])
            # print(df.shape[0])
            
            if not single_db and ((errors['data_errors'] == [] and errors['info_errors'] == []) or not save_db_only_without_errors):
                # Save the processed DataFrame to the output folder
                file_output_path = os.path.join(output_folder, file)
                df.to_excel(file_output_path, sheet_name='Total Data')
                print(f"Анкета {file} сохранена в {output_folder}!")
            if errors['data_errors'] != [] or errors['info_errors'] != []:
                unprocessed_files[os.path.basename(file_path)] = errors
            else:
                print("В файле не обнаружено ошибок, мои поздравления!")
    if single_db and not save_db_only_without_errors:
        file_output_path = os.path.join(output_folder, 'result_db.xlsx')
        result_df.to_excel(file_output_path, sheet_name='Total Data')
        print(f"Все анкеты объединены в {output_folder}!")

    return unprocessed_files