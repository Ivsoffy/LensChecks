# All the variables are imported from LP.py file
import os
import re
import shutil
import sys
import time
import warnings

import numpy as np
import pandas as pd
import win32com.client
from win32com.client import makepy

# warnings.filterwarnings("ignore", category=UserWarning)
warnings.simplefilter("ignore", category=UserWarning, lineno=329, append=False)
warnings.filterwarnings(
    "ignore",
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
    category=FutureWarning,
)
pd.set_option("future.no_silent_downcasting", True)
parent_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, parent_dir)

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from .. import LP  # noqa: E402
from ..utils import (  # noqa: E402
    convert_some_columns_to_numeric,
    convert_some_columns_to_str,
    has_errors,
    init_errors,
    is_empty_value,
    is_excel_file,
    main_checks,
    normalize_column_names,
    prepare_total_data,
)


def get_valid_path(prompt):
    while True:
        path = input(prompt)
        path = path.replace("\\", "/")  # Replace backslashes with forward slashes
        if os.path.isdir(path):
            return path
        else:
            print("Invalid path. Please try again.")


def check_general_info(errors, df):
    # Setting columns names to the russian version

    for col, value in df.loc[
        1,
        [
            LP.company_name,
            LP.gi_sector,
            LP.gi_origin,
            LP.gi_headcount_cat,
            LP.gi_revenue_cat,
        ],
    ].items():
        # value = row[col]                     # Значение
        if is_empty_value(value):
            errors["info_errors"].append(f"Incorrect General Info: {col}")

    comp_name = df[LP.company_name][0]
    if not re.fullmatch(r"[A-Za-z0-9_]+", str(comp_name)):
        errors["info_errors"] += [f"Incorrect company name format: {comp_name}"]
        # print(df[gi_company_name

    # df['SDF Language'] = lang
    return errors, df


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
    info = errors.get("info_errors", [])
    data = [col for col, _ in errors.get("data_errors", [])]
    unique_data = list(dict.fromkeys(data))
    n = max(len(info), len(unique_data))
    df_errors = pd.DataFrame(
        {
            "info_errors": info + [None] * (n - len(info)),
            "data_errors": unique_data + [None] * (n - len(unique_data)),
        }
    )

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
    header_range.Font.Color = 0xFFFFFF  # Белый
    header_range.Interior.Color = 0x4472C4  # Синий (BGR)
    header_range.HorizontalAlignment = -4108  # xlCenter
    header_range.Borders.Weight = 2

    # --- Форматирование тела таблицы ---
    used_range = ws_err.UsedRange
    used_range.Columns.AutoFit()
    for row in ws_err.Range(
        ws_err.Cells(2, 1), ws_err.Cells(df_errors.shape[0] + 1, 2)
    ):
        row.Borders.Weight = 2
        row.WrapText = True
        row.VerticalAlignment = -4160  # xlTop

    # --- Определение листа "Данные" ---
    data_sheet = None
    for s in wb.Sheets:
        name = s.Name.strip().lower()
        if name in ("данные", "salary data", "total data"):
            data_sheet = s
            break
    if not data_sheet:
        wb.Close(SaveChanges=False)
        excel.Quit()
        raise ValueError("Не найден лист 'Данные'.")

    ws_data = data_sheet

    # --- Определяем начало данных ---
    # Treat sheet as a regular DataFrame: headers are on the first row.
    df_data = pd.read_excel(input_path, sheet_name=ws_data.Name, header=0)

    def norm(s):
        return str(s).strip().lower() if pd.notna(s) else ""

    col_map = {
        norm(col_name): col_idx + 1 for col_idx, col_name in enumerate(df_data.columns)
    }
    row_map = {idx: idx + 2 for idx in range(len(df_data))}
    try:
        df_data_indexed = pd.read_excel(
            input_path, sheet_name=ws_data.Name, header=0, index_col=0
        )
        row_map_by_label = {
            label: pos + 2 for pos, label in enumerate(df_data_indexed.index)
        }
    except Exception:
        row_map_by_label = {}

    # --- Подсветка ошибок ---
    orange_color = 0x00C0FF  # BGR = (0xFF, 0xC0, 0x00) → оранжевый
    for col_name, idx in errors.get("data_errors", []):
        col_idx = col_map.get(norm(col_name))
        if not col_idx:
            print(f"Не найдена колонка: {col_name}")
            continue
        excel_row = row_map_by_label.get(idx)
        if excel_row is None:
            excel_row = row_map.get(idx)
        if excel_row is None:
            try:
                excel_row = row_map_by_label.get(int(idx), row_map.get(int(idx)))
            except (TypeError, ValueError):
                excel_row = None

        if excel_row is None:
            print(f"Row for error index was not found: {idx}")
            continue
        if 2 <= excel_row <= ws_data.UsedRange.Rows.Count:
            ws_data.Cells(excel_row, col_idx).Interior.Color = orange_color
        else:
            print(f"Строка вне диапазона: {excel_row}")

    # --- Сохранение и закрытие ---
    wb.SaveAs(output_path)
    wb.Close(SaveChanges=True)
    excel.Quit()

    print(f"Лист 'Errors' добавлен, ячейки подсвечены. Файл: {output_path}")


def check_one_interval(errors, grade_num, val, min, max, index, col):
    if not is_empty_value(val):
        try:
            if not (min[grade_num] < val < max[grade_num]):
                errors["data_errors"] += [(col, index)]
        except Exception:
            errors["data_errors"] += [(LP.grade, index)]
    return val


def check_intervals(errors, df):
    cols_to_check = [LP.base_pay, LP.tc_pay, LP.ttc_pay, LP.tdc_pay, LP.target_sti_out]

    intervals_path = "modules/module_5/intervals.parquet"
    if not os.path.exists(intervals_path):
        raise FileNotFoundError(f"Ошибка: файл intervals не найден: {intervals_path}")
    intervals = pd.read_parquet(intervals_path)
    intervals = intervals.set_index(intervals.columns[0])

    for col in cols_to_check:
        col_min = col + "_Min"
        col_max = col + "_Max"
        df[col] = df.apply(
            lambda x: check_one_interval(
                errors,
                x[LP.grade],
                x[col],
                intervals[col_min],
                intervals[col_max],
                x.name,
                col,
            ),
            axis=1,
        )


def find_outliers_iqr(data):
    data = np.array(data)

    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    iqr = q3 - q1

    lower_bound = max(0, q1 - 1.5 * iqr)
    upper_bound = q3 + 1.5 * iqr

    outlier_mask = (data < lower_bound) | (data > upper_bound)
    return lower_bound, upper_bound, outlier_mask


def get_outlier_strength(values, lower_bound, upper_bound):
    values = np.asarray(values, dtype=float)
    return np.where(values < lower_bound, lower_bound - values, values - upper_bound)


def check_outliers(errors, df):
    cols_to_check = [LP.base_pay, LP.tc_pay, LP.ttc_pay]
    df["outlier"] = False
    for col in cols_to_check:
        df[f"{col}_lower_bound"] = np.nan
        df[f"{col}_upper_bound"] = np.nan

    outlier_candidates = []

    for col in cols_to_check:
        for _, group_df in df.groupby(LP.grade):
            if len(group_df) < 5:
                continue

            series = group_df[col].dropna()
            if series.empty:
                continue

            lower_bound, upper_bound, outlier_mask = find_outliers_iqr(
                series.to_numpy()
            )
            if not outlier_mask.any():
                continue

            outlier_series = series[outlier_mask]
            outlier_strength = pd.Series(
                get_outlier_strength(
                    outlier_series.to_numpy(), lower_bound, upper_bound
                ),
                index=outlier_series.index,
            )

            for ind, strength in outlier_strength.items():
                outlier_candidates.append(
                    {
                        "col": col,
                        "ind": ind,
                        "strength": float(strength),
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound,
                    }
                )

    if not outlier_candidates:
        return df

    outlier_candidates_df = pd.DataFrame(outlier_candidates)
    top_count = max(1, int(np.ceil(len(outlier_candidates_df) * 0.05)))
    top_outliers = outlier_candidates_df.nlargest(top_count, "strength")

    for _, row in top_outliers.iterrows():
        col = row["col"]
        ind = row["ind"]
        errors["data_errors"] += [(col, ind)]
        df.loc[ind, "outlier"] = True
        df.loc[ind, f"{col}_lower_bound"] = row["lower_bound"]
        df.loc[ind, f"{col}_upper_bound"] = row["upper_bound"]

    return df


def check_and_process_data(errors, df, params):
    df = convert_some_columns_to_numeric(df)
    df = convert_some_columns_to_str(df)

    main_checks(errors, df)

    # Проверка компенсационных элементов на универсальные интервалы
    check_intervals(errors, df)
    # Проверка компенсационных элементов на выбросы относительно практики компании
    check_outliers(errors, df)

    return errors, df


def _process_single_file(file_path, params):
    """Read, validate and process one SDF file."""
    errors = init_errors()
    df = pd.read_excel(file_path, sheet_name="Total Data", index_col=0)
    df = normalize_column_names(df)

    missing_columns = [
        col for col in LP.expected_columns_market_df_preload if col not in df.columns
    ]
    if missing_columns:
        errors["info_errors"].append(
            f"Missing required columns in 'Total Data': {missing_columns}"
        )
        return None, errors

    df = prepare_total_data(df, [LP.company_name, LP.job_title])
    print("Checking General Info section...")
    errors, df = check_general_info(errors, df)
    print("Checking employee data...")
    errors, df = check_and_process_data(errors, df, params)
    return df, errors


def _save_processed_file(df, file_name, output_folder):
    """Save one processed file to the output folder."""
    file_output_path = os.path.join(output_folder, file_name)
    df = df.loc[:, LP.expected_columns_market_df]
    df.to_excel(file_output_path, sheet_name="Total Data")
    print(f"File {file_name} was saved to {output_folder}.")


def _save_single_db(dataframes, output_folder):
    """Save concatenated dataframes to result_db.xlsx."""
    if not dataframes:
        return

    result_df = pd.concat(dataframes)
    result_df = result_df.loc[:, ~result_df.columns.str.contains("^Unnamed")]
    file_output_path = os.path.join(output_folder, "result_db.xlsx")
    result_df.to_excel(file_output_path, sheet_name="Total Data")
    print(f"Combined database was saved to {output_folder}.")


def _print_unprocessed_summary(unprocessed_files):
    """Print summary for files that contain validation issues."""
    if not unprocessed_files:
        print("\nAll files were validated successfully.")
        return

    print("=" * 20 + " WARNING! " + "=" * 20)
    print("List of unprocessed files:")
    for file_name, issue in unprocessed_files.items():
        data_err = [col for col, _ in issue.get("data_errors", [])]
        unique_data_err = list(dict.fromkeys(data_err))
        print(
            f"\nFile: {file_name}, Info errors: {issue['info_errors']}\nData errors: {unique_data_err}"
        )


def _save_unprocessed_files(unprocessed_files, output_folder):
    """Copy files with issues to 'unprocessed' and write error details to Excel."""
    unprocessed_folder = os.path.join(output_folder, "unprocessed")
    os.makedirs(unprocessed_folder, exist_ok=True)
    if not unprocessed_files:
        return

    print(f"\nSaving {len(unprocessed_files)} files to 'unprocessed'...")
    for file_name, issue in unprocessed_files.items():
        source_path = os.path.join(output_folder, file_name)
        destination_path = os.path.join(unprocessed_folder, file_name)
        try:
            if not os.path.exists(source_path):
                continue
            if os.path.exists(destination_path):
                os.remove(destination_path)
            add_errors_to_excel(issue, source_path, destination_path)
        except Exception as error:
            print(f"Failed to save {file_name} to unprocessed: {str(error)}")


def module_5(input_folder, output_folder, params=None):
    """Run final checks, save processed files, and separate files with issues."""
    print("Module 5: final validation.")
    process_start = time.time()

    unprocessed_files = file_processing(input_folder, output_folder, params)
    process_end = time.time()
    print(f"File processing took: {process_end - process_start}")

    _print_unprocessed_summary(unprocessed_files)
    _save_unprocessed_files(unprocessed_files, output_folder)
    return unprocessed_files


def file_processing(input_folder, output_folder, params):
    """Process all Excel files from input_folder and return files with errors."""
    params = params or {}
    unprocessed_files = {}
    result_frames = []
    single_db = params.get("single_db", False)
    save_db_only_without_errors = params.get("save_db_only_without_errors", False)

    excel_files = [
        file_name for file_name in os.listdir(input_folder) if is_excel_file(file_name)
    ]
    for counter, file_name in enumerate(excel_files, start=1):
        print(f"Processing file {counter}: {file_name}")
        file_path = os.path.join(input_folder, file_name)
        df, errors = _process_single_file(file_path, params)
        has_validation_errors = has_errors(errors)

        if df is not None:
            should_save = (not has_validation_errors) or (
                not save_db_only_without_errors
            )
            if single_db and should_save:
                result_frames.append(df)
            if not single_db and should_save:
                _save_processed_file(df, file_name, output_folder)

        if has_validation_errors:
            unprocessed_files[file_name] = errors
        else:
            print("No validation issues were found in this file.")

    if single_db:
        _save_single_db(result_frames, output_folder)

    return unprocessed_files
