
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
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

pd.set_option('future.no_silent_downcasting', True)
parent_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, parent_dir)
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from LP import *
from inference import predict_codes

cols = [company_name, dep_level_1, dep_level_2, dep_level_3, dep_level_4, dep_level_5, dep_level_6,
                                job_title]

def process_past_year(folder_py, df):
    companies = df[company_name].unique()

    if not isinstance(folder_py,str) or not os.path.exists(folder_py):
        print(f"Папка {folder_py} с анкетами прошлого года не найдена")
    else:
        for company in companies:
            print(f"Ищем анкету с прошлого года для компании {company}")
            try:
                found_files = check_if_past_year_exist(company, folder_py)
                if found_files:
                    file_to_cmp = os.path.join(folder_py, found_files[0])
                    df_py = pd.read_excel(file_to_cmp, sheet_name=rem_data, header=6, index_col=0)
                    cols_to_copy = [function_code, subfunction_code, specialization_code, function, subfunction, specialization]
                    # Заполняем данными с прошлого года
                    df = merge_by_cols(df, df_py, cols, cols_to_copy)
            except:
                print(f"Не удалось прочитатать файл {found_files[0]}")
    return df

def module_2(input_folder, output_folder, params):
    folder_py = params['folder_past_year']
    already_fixed = params['after_fix']
    
    counter = 0
    found_files=[]
    for file in os.listdir(input_folder):
        # Check if the file is an Excel file
        if file.endswith('.xlsx') or file.endswith('.xls') or file.endswith('.xlsm'):
            counter+=1
            output_file = os.path.join(output_folder, file)
            file_path = os.path.join(input_folder, file)

            print(f"Проверяем файл {counter}: {file}")
            df = pd.read_excel(file_path, sheet_name='Total Data', index_col=0)

            if not already_fixed: # Первичная обработка
                # Подтягиваем коды с прошлого года
                df = process_past_year(folder_py,df)
                
                # Делим данные на заполненные и незаполненные
                unfilled = df.loc[
                    df[function_code]
                    .astype(str)
                    .str.lower()
                    .str.strip()
                    .isin(['nan', 'none', 'null', ''])
                ]
                filled = df[~df.index.isin(unfilled.index)]
                empty_count = unfilled.shape[0]

                print(f"Проставлено кодов: {len(filled)}, отсутствует кодов: {len(unfilled)}")

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
    """

    try:
        # --- читаем оба листа ---
        try:
            df_prefill = pd.read_excel(excel_file, sheet_name=sheet_prefill)
            df_target = pd.read_excel(excel_file, sheet_name=sheet_target)
        except FileNotFoundError:
            print(f"Ошибка: файл '{excel_file}' не найден.")
            return
        except ValueError as e:
            print(f"Ошибка при чтении листов: {e}")
            return
        except Exception as e:
            print(f"Не удалось прочитать Excel-файл: {e}")
            return

        if match_cols is None:
            match_cols = [col for col in df_prefill.columns if col not in code_cols]

        # приведение типов к строке для колонок совпадений
        for col in match_cols:
            if col in df_prefill.columns:
                df_prefill[col] = df_prefill[col].astype(str).fillna('')
            if col in df_target.columns:
                df_target[col] = df_target[col].astype(str).fillna('')

        if set(match_cols).issubset(df_prefill.columns) and set(match_cols).issubset(df_target.columns):
            try:
                df_merged = df_target.merge(
                    df_prefill[match_cols + list(code_cols)],
                    on=match_cols,
                    how='left',
                    suffixes=('', '_prefill')
                )
            except KeyError as e:
                print(f"Ошибка при объединении таблиц: отсутствует колонка {e}")
                return
            except Exception as e:
                print(f"Ошибка при объединении данных: {e}")
                return

            # --- заменяем коды из Prefill, если там есть значения ---
            for col in code_cols:
                try:
                    df_merged[col] = df_merged[f"{col}_prefill"].combine_first(df_merged[col])
                    df_merged.drop(columns=f"{col}_prefill", inplace=True)
                except KeyError:
                    print(f"Предупреждение: колонка '{col}' отсутствует в данных для замены.")
                except Exception as e:
                    print(f"Ошибка при обработке колонки '{col}': {e}")

            # сохраняем результат
            try:
                if not os.path.exists(output_path):
                    with pd.ExcelWriter(output_path, engine="openpyxl", mode="w") as writer:
                        df_merged.to_excel(writer, sheet_name=sheet_target, index=False)
                else:
                    with pd.ExcelWriter(output_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
                        df_merged.to_excel(writer, sheet_name=sheet_target, index=False)

                print(f"На лист '{sheet_target}' подтянуты значения из листа '{sheet_prefill}' в файле {excel_file}")
            except PermissionError:
                print(f"Ошибка: нет доступа для записи в файл '{output_path}'. Возможно, он открыт.")
            except Exception as e:
                print(f"Ошибка при сохранении файла: {e}")
        else:
            print("Не все колонки из match_cols найдены в обоих листах.")
    except Exception as e:
        print(f"Непредвиденная ошибка при выполнении функции: {e}")

        
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

    # --- Обработка df1 ---
    base_cols_1 = [
        company_name, 'Сектор', function_code, subfunction_code, specialization_code,
        dep_level_1, dep_level_2, dep_level_3, dep_level_4,
        dep_level_5, dep_level_6, job_title
    ]
    extra_cols_1 = ['past_year_check', 'func_old', 'subfunc_old', 'spec_old']
    df1_cols = base_cols_1 + extra_cols_1 if 'func_old' in df1.columns else base_cols_1
    df1 = df1[[c for c in df1_cols if c in df1.columns]]

    # --- Обработка df2 ---
    base_cols_2 = [
        company_name, 'Сектор', function_code, subfunction_code, specialization_code,
        dep_level_1, dep_level_2, dep_level_3, dep_level_4,
        dep_level_5, dep_level_6, job_title
    ]
    conf_cols = ['function_confidence', 'subfunction_confidence', 'specialization_confidence']
    df2_cols = base_cols_2[:5] + conf_cols + base_cols_2[5:] if 'function_confidence' in df2.columns else base_cols_2
    df2 = df2[[c for c in df2_cols if c in df2.columns]]

    try:
        book = load_workbook(output_file)
    except FileNotFoundError:
        print(f"Ошибка: файл '{output_file}' не найден.")
        return
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
    return df_merged


def check_if_past_year_exist(company, folder_py):
    company_str = str(company).strip()
    found_files = []

    for filename in os.listdir(folder_py):
        if company_str.lower() in filename.lower():
            found_files.append(filename)
    
    if found_files:
        for f in found_files:
            print(f"Найдена анкета прошлого года: {f}")
    else:
        print("Не найдено анкет прошлого года.")
    return found_files