
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
warnings.simplefilter(action='ignore', category=FutureWarning)

pd.set_option('future.no_silent_downcasting', True)
parent_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, parent_dir)
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from LP import *
from inference import predict_codes

def module_2(input_folder, output_folder, params):
    folder_py = params['folder_past_year']
    already_fixed = params['after_fix']
    lang = ''
    counter = 0
    for file in os.listdir(input_folder):
        # Check if the file is an Excel file
        if file.endswith('.xlsx') or file.endswith('.xls') or file.endswith('.xlsm'):
            counter+=1
            output_file = os.path.join(output_folder, file)
            file_path = os.path.join(input_folder, file)

            print(f"Processing file {counter}: {file}")
            df = pd.read_excel(file_path, sheet_name='Total Data')
            # df.to_excel(output_file)
            cols = [company_name, dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                                job_title]
            companies = df[company_name].unique()

            if not already_fixed: # Первичная обработка
                for company in companies:
                    found_files = check_if_past_year_exist(company, folder_py)
                    if found_files:
                        file_to_cmp = os.path.join(folder_py, found_files[0])
                        df_py = pd.read_excel(file_to_cmp, sheet_name=rem_data, header=6)
                        cols_to_copy = [function_code, subfunction_code, specialization_code, function, subfunction, specialization]
                        # Заполняем данными с прошлого года
                        df = merge_by_cols(df, df_py, cols, cols_to_copy)
            
                
                # Делим данные на заполненные и незаполненные
                unfilled = df.loc[df[function_code].apply(lambda x: str(x).lower().strip() == 'nan') == True] #add subfunction
                filled = df[~df.index.isin(unfilled.index)]
                empty_count = unfilled.shape[0]

                print(f"filled: {len(filled)}, unfilled: {len(unfilled)}")

                filled_and_processed = process_filled(filled)
                df, unfilled_and_processed, count_past_year, count_model = process_unfilled(unfilled, df)
                df.to_excel(output_file, sheet_name='Total Data')


                process_output_file(filled_and_processed, unfilled_and_processed, cols, output_file)

                info = {
                    'Файлы с прошлого года': str(found_files) if found_files else 'Отсутствуют',
                    'Отсутствующих кодов в файле':  empty_count, 
                    'Всего строк в файле': df.shape[0],
                    'Подтянуто из прошлого года': count_past_year,
                    'Проставлено нейросетью': count_model
                }

                add_info(info, output_file)
            else: # Аналитик проверил и исправил анкету
                
                map_prefill_to_sheet1(file_path, output_file, sheet_prefill='Prefill')
                map_prefill_to_sheet1(file_path, output_file, sheet_prefill='Model')
        print("-----------------------")
        print()


def map_prefill_to_sheet1(
    excel_file: str,
    output_path,
    sheet_prefill,
    match_cols=[company_name, dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6, job_title],
    code_cols=(function_code, subfunction_code, specialization_code),
    sheet_target='Total Data'
):
    """
    Маппит значения кодов из листа Prefill на данные в листе Sheet1 по совпадению колонок.
    
    Параметры:
        excel_file (str): путь к Excel-файлу
        match_cols (list): список колонок, по которым искать совпадения
        code_cols (tuple): коды, которые нужно обновить
        sheet_prefill (str): имя листа с уникальными значениями
        sheet_target (str): имя листа с основной таблицей (Total Data)
    
    Возвращает:
        pd.DataFrame: обновлённый датафрейм из Sheet1
    """

    # --- читаем оба листа ---
    df_prefill = pd.read_excel(excel_file, sheet_name=sheet_prefill)
    df_target = pd.read_excel(excel_file, sheet_name=sheet_target)

    # если колонки не заданы — пытаемся угадать автоматически
    if match_cols is None:
        match_cols = [col for col in df_prefill.columns if col not in code_cols]

    if set(match_cols).issubset(df_prefill):

    # --- делаем merge ---
        df_merged = df_target.merge(
            df_prefill[match_cols + list(code_cols)],
            on=match_cols,
            how='left',
            suffixes=('', '_prefill')
        )

        # --- заменяем коды из Prefill, если там есть значения ---
        for col in code_cols:
            df_merged[col] = df_merged[f"{col}_prefill"].combine_first(df_merged[col])
            df_merged.drop(columns=f"{col}_prefill", inplace=True)

    # wb = load_workbook(excel_file)
    # ws = wb[sheet_prefill]
    # wb.remove(ws)
    # print(f"Лист '{sheet_prefill}' удалён.")
    # wb.save(excel_file)
        if not os.path.exists(output_path):
            # Если файла нет — создаём новый
            with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
                df_merged.to_excel(writer, sheet_name=sheet_target, index=False)
        else:
            # Если файл есть — добавляем или заменяем лист
            with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                df_merged.to_excel(writer, sheet_name=sheet_target, index=False)
        print(f"На лист '{sheet_target}' подтянуты значения из листа {sheet_prefill} в файле {excel_file}")

        # return df_merged

        
def add_info(info, output_file):
    info = pd.DataFrame(data=[info])
    book = load_workbook(output_file)

    ws3 = book.create_sheet(title='Info')

    # Записываем заголовки
    for col_idx, col_name in enumerate(info.columns, start=1):
        ws3.cell(row=1, column=col_idx, value=col_name)

    # Записываем строки
    for row in info.itertuples(index=False):
        excel_row = ws3.max_row + 1
        for col_idx, value in enumerate(row, start=1):
            ws3.cell(row=excel_row, column=col_idx, value=value)
    book.save(output_file)


def process_output_file(df1, df2, cols, output_file, sheet1_name='Prefill', sheet2_name='Model'):
    """
    Добавляет два датафрейма в существующий Excel-файл.
    В df1 подсвечивает красным строки, где past_year_check == False.
    В df2 подсвечивает красным строки, где function_confidence < 70.
    """

    # Оставляем только уникальные строки
    df1 = df1.drop_duplicates(subset=cols)
    df2 = df2.drop_duplicates(subset=cols)

    # df1 = df1.loc[:, [company_name, function_code, subfunction_code, specialization_code]]
    # print("OK")
    # cols_df1 = [company_name, function_code, subfunction_code, specialization_code,
    #            'past_year_check', dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
    #                             job_title]
    # print(all(c in df1.columns for c in cols_df1))
    # # print(df2.columns)

    if 'func_old' in df1.columns:
        # print(1)
        df1 = df1.loc[:, [company_name, function_code, subfunction_code, specialization_code,
               'past_year_check', dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                                job_title, 'func_old', 'subfunc_old', 'spec_old']]
    else:
        # print(2)
        df1 = df1.loc[:, [company_name, function_code, subfunction_code, specialization_code,
                        dep_level_1,
        dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                        job_title]]
    
    if len(df2.columns)>1:
        if 'function_confidence' in df2.columns:
            # print(3)
            df2 = df2.loc[:, [company_name, 'Сектор', function_code, subfunction_code, specialization_code,
                'function_confidence', 'subfunction_confidence', 'specialization_confidence',
                    dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                                    job_title]]
        else:
            # print(4)
            df2 = df2.loc[:, [company_name, 'Сектор', function_code, subfunction_code, specialization_code,
                    dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                                    job_title]]

    book = load_workbook(output_file)
    red_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')

    # Работа с df1
    ws1 = book.create_sheet(title=sheet1_name)
    for col_idx, col_name in enumerate(df1.columns, start=1):
        ws1.cell(row=1, column=col_idx, value=col_name)

    for _, row in df1.iterrows():
        excel_row = ws1.max_row + 1
        highlight = row.get('past_year_check') is False

        for col_idx, value in enumerate(row, start=1):
            cell = ws1.cell(row=excel_row, column=col_idx, value=value)
            if highlight:
                cell.fill = red_fill

    # Работа с df2
    ws2 = book.create_sheet(title=sheet2_name)
    for col_idx, col_name in enumerate(df2.columns, start=1):
        ws2.cell(row=1, column=col_idx, value=col_name)

    for _, row in df2.iterrows():
        excel_row = ws2.max_row + 1
        try:
            s = str(row.get('function_confidence'))
            num = float(s.rstrip('%'))
            highlight = num < 70
        except:
            highlight = False

        for col_idx, value in enumerate(row, start=1):
            cell = ws2.cell(row=excel_row, column=col_idx, value=value)
            if highlight:
                cell.fill = red_fill

    book.save(output_file)
    print(f"Листы '{sheet1_name}' и '{sheet2_name}' добавлены в файл: {output_file}")

def process_unfilled(df, df_orig):
    # Подтягиваем коды прошлых лет в оригинальный датасет
    count_past_year = 0
    preds = pd.DataFrame()

    if 'func_old' in df_orig.columns:
        df_orig[function_code].update(df['func_old'])
        df_orig[subfunction_code].update(df['subfunc_old'])
        df_orig[specialization_code].update(df['spec_old'])

    df_without_py = df_orig.loc[df_orig[function_code].apply(lambda x: str(x).lower().strip() == 'nan') == True]
    count_model = df_without_py.shape[0]
    count_past_year = df.shape[0] - count_model
    # Там где прошлого года нет, проставляем нейронкой
    if count_model != 0:
        preds = predict_codes(df_without_py)
        preds = preds.loc[preds[company_name].apply(lambda x: str(x).lower().strip() == 'nan') == False]

    return df_orig, preds, count_past_year, count_model
    

def process_filled(df):
    """
    Сравнивает столбцы 'function_code' и 'func_old' в датафрейме.
    Создаёт новый столбец 'past_year_check', где:
      - True, если значения совпадают,
      - True, если значение func_old == NaN,
      - False — если различаются.
    """
    df = df.copy()
    df["past_year_check"] = True

    if "func_old" in df.columns:
        df["past_year_check"] = (
            (df[function_code] == df["func_old"]) |
            (df["func_old"].isna())
        )
    return df


def merge_by_cols(df, df_py, cols, cols_to_copy):
    """
    Сравнивает строки df и df_py по списку колонок cols.
    Если значения совпадают — копирует значения колонок cols_to_copy из df_py в df.

    Параметры:
        df (pd.DataFrame): основной датафрейм, в который копируем данные
        df_py (pd.DataFrame): источник данных
        cols (list): список колонок для сравнения
        cols_to_copy (list): список колонок, которые нужно скопировать из df_py в df

    Возвращает:
        pd.DataFrame: обновлённый df
    """

    print(f"Shape до merge: {df.shape[0]}")

    missing_cols = [c for c in cols + cols_to_copy if c not in df_py.columns]
    if missing_cols:
        raise ValueError(f"Отсутствуют колонки в df_py: {missing_cols}")

    # Уберём дубликаты по ключевым колонкам в df_py
    df_py_unique = df_py.drop_duplicates(subset=cols, keep="first")

    df_merged = df.merge(
        df_py_unique[cols + cols_to_copy],
        on=cols,
        how="left",
        suffixes=("", "_py")
    )

    func_cols = ['func_old', 'subfunc_old', 'spec_old']

    for old_col, new_col in zip(cols_to_copy, func_cols):
        py_col = f"{old_col}_py"
        if py_col in df_merged.columns:
            df_merged[new_col] = df_merged[py_col]
            df_merged.drop(columns=[py_col], inplace=True)
        else:
            df_merged[new_col] = np.nan

    print(f"Shape после merge: {df_merged.shape[0]}")
    return df_merged
    

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

    df_diff = df.merge(df_py, how='left', indicator=True)
    df_missing = df_diff.query('_merge == "left_only"').drop(columns=['_merge'])

    set_py = set(tuple(row) for row in df_py.itertuples(index=False, name=None))

    missing_indices = []
    for idx, row in df.iterrows():
        tup = tuple(row[c] for c in df.columns)
        if tup not in set_py:
            missing_indices.append(idx)

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


def check_if_codes_exist(df):
    empty_count = df[function_code].isna().sum()
    # non_empty_count = df.loc[df[company_name]==company][function_code].notna().sum()
    return empty_count == 0

def check_if_past_year_exist(company, folder_py):
    company_str = str(company).strip()
    found_files = []
    
    for filename in os.listdir(folder_py):
        if company_str.lower() in filename.lower():
            found_files.append(filename)
    
    if found_files:
        for f in found_files:
            print(f"Найден файл: {f}")
    return found_files