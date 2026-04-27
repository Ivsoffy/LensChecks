import re
import warnings

import numpy as np
import pandas as pd

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
    grouped = (
        df.groupby(group_cols)
        .agg(
            total_salary=("BP", "sum"),
            headcount=("job_title", "count"),
        )
        .reset_index()
    )
    grouped[out_col] = grouped["total_salary"] / grouped["headcount"]
    return df.merge(grouped[group_cols + [out_col]], on=group_cols, how="left")


def add_company_salary_sum(df):
    grouped = (
        df.groupby(["company", "region"])
        .agg(salary_sum_comp=("BP", "sum"))
        .reset_index()
    )
    return df.merge(grouped, on=["company", "region"], how="left")


def is_empty_value(x):
    """Возвращает True, если значение можно считать пустым."""
    if x is None:
        return True
    if isinstance(x, (float, np.floating)) and pd.isna(x):
        return True
    if isinstance(x, str):
        s = x.strip().lower()
        if s in ("", "nan", "none", "null", "n/a", "na", "-", "--"):
            return True
    if isinstance(x, (list, tuple, dict, set)) and len(x) == 0:
        return True
    return False


def sanitize_text(text):
    if is_empty_value(text):
        return ""
    text = str(text).lower()
    text = re.sub(r"[^a-zа-я0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


VALID_N_LEVELS = {"N", "N-1", "N-2", "N-3", "N-4"}
N_MINUS_1_FUNCTION_CODES = {"EMA-B", "EMB", "EMD", "EMX"}
N_MINUS_1_TITLE_PATTERNS = (
    "директор департамента",
    "руководитель департамента",
    "директор филиала",
    "руководитель филиала",
)


JOB_TITLE_FILTER_KEYWORDS = (
    "руководитель",
    "директор",
    "директора",
    "начальник",
    "head of",
    "лидер",
    "lead",
    "президент",
    "менеджер",
    "manager",
    "техлид",
    "управляющий",
)


LEAD_ROLE_CODES = {"EMA-A", *N_MINUS_1_FUNCTION_CODES}


def _normalize_code(value):
    if is_empty_value(value):
        return ""
    return str(value).strip().upper()


def _normalize_n_level(value):
    if is_empty_value(value):
        return ""
    normalized = _normalize_code(value)
    if normalized in VALID_N_LEVELS:
        return normalized
    return "N-4"


def _get_department_columns(df):
    department_cols = [
        col
        for col in df.columns
        if col.startswith("Подразделение ") and col.endswith(" уровня")
    ]
    return sorted(
        department_cols, key=lambda col: int(re.search(r"(\d+)", col).group(1))
    )


def _calculate_department_stats(df, department_cols):
    company_department_count = pd.Series(0, index=df.index, dtype="int64")
    row_department_count = pd.Series(0, index=df.index, dtype="int64")

    if not department_cols or "company" not in df.columns:
        return company_department_count, row_department_count

    for _, company_idx in df.groupby("company", dropna=False).groups.items():
        company_idx = list(company_idx)
        company_df = df.loc[company_idx, department_cols]
        informative_cols = []

        for col in department_cols:
            normalized = company_df[col].map(
                lambda value: ""
                if is_empty_value(value)
                else str(value).strip().lower()
            )
            non_empty = normalized[normalized != ""]
            if non_empty.empty or non_empty.nunique() <= 1:
                continue
            informative_cols.append(col)

        company_department_count.loc[company_idx] = len(informative_cols)
        if not informative_cols:
            continue

        def count_unique_departments(row):
            values = {
                str(value).strip().lower() for value in row if not is_empty_value(value)
            }
            return len(values)

        row_department_count.loc[company_idx] = (
            company_df[informative_cols]
            .apply(count_unique_departments, axis=1)
            .astype("int64")
        )

    return company_department_count, row_department_count


def calculate_n_level(df):
    df = df.copy()
    if "n_level" not in df.columns:
        df["n_level"] = ""

    n_level = df["n_level"].map(_normalize_n_level)
    unresolved_mask = n_level.eq("")

    role_codes = (
        df["code"].map(_normalize_code)
        if "code" in df.columns
        else pd.Series("", index=df.index)
    )
    job_titles = (
        df["job_title"].map(sanitize_text)
        if "job_title" in df.columns
        else pd.Series("", index=df.index)
    )
    seniority = (
        pd.to_numeric(df["seniority"], errors="coerce")
        if "seniority" in df.columns
        else pd.Series(np.nan, index=df.index)
    )
    department_cols = _get_department_columns(df)
    company_department_count, row_department_count = _calculate_department_stats(
        df, department_cols
    )

    n_level.loc[unresolved_mask & role_codes.eq("EMA-A")] = "N"

    unresolved_mask = n_level.eq("")
    n_minus_1_by_function = role_codes.isin(N_MINUS_1_FUNCTION_CODES)
    n_level.loc[unresolved_mask & n_minus_1_by_function] = "N-1"

    unresolved_mask = n_level.eq("")
    n_minus_1_by_title = job_titles.map(
        lambda title: any(pattern in title for pattern in N_MINUS_1_TITLE_PATTERNS)
    )
    n_level.loc[unresolved_mask & n_minus_1_by_title] = "N-1"

    unresolved_mask = n_level.eq("")
    complex_company_mask = company_department_count.gt(1)
    leader_mask = seniority.eq(4)
    n_level.loc[
        unresolved_mask
        & complex_company_mask
        & leader_mask
        & row_department_count.eq(1)
    ] = "N-1"

    unresolved_mask = n_level.eq("")
    n_level.loc[
        unresolved_mask
        & complex_company_mask
        & leader_mask
        & row_department_count.eq(2)
    ] = "N-2"

    df["n_level"] = n_level
    return df


def _normalize_title_for_filter(value):
    if is_empty_value(value):
        return ""
    normalized = re.sub(r"[^\w\s]", " ", str(value).lower())
    return re.sub(r"\s+", " ", normalized).strip()


def _has_allowed_job_title(value):
    normalized_title = _normalize_title_for_filter(value)
    return any(keyword in normalized_title for keyword in JOB_TITLE_FILTER_KEYWORDS)


def _get_first_existing_column(df, *candidates):
    for candidate in candidates:
        if candidate in df.columns:
            return df[candidate]
    return pd.Series("", index=df.index, dtype="object")


def _resolve_role_codes(df):
    if "code" in df.columns:
        return df["code"].map(_normalize_code)

    spec = _get_first_existing_column(df, "spec", "Код специализации")
    subfunction = _get_first_existing_column(df, "subfunction", "Код подфункции")
    raw_code = spec.where(~spec.map(is_empty_value), subfunction)
    return raw_code.map(_normalize_code)


def get_lead_mask(df):
    if df.empty:
        return pd.Series(dtype="bool", index=df.index)

    title_series = _get_first_existing_column(df, "job_title", "Название должности")
    function_codes = _get_first_existing_column(df, "function", "Код функции").map(
        _normalize_code
    )
    role_codes = _resolve_role_codes(df)

    title_mask = title_series.map(_has_allowed_job_title)
    code_mask = role_codes.isin(LEAD_ROLE_CODES) | function_codes.str.endswith("Z")
    return title_mask | code_mask


def calculate_f_new(df):
    df.rename(
        columns={
            "Название компании (заполняется автоматически)": "company",
            "Сектор": "industry",
            "Общее количество сотрудников по состоянию на 1 мая 2025 года": "headcount_cat",
            "Выручка за 2024 год, руб.": "revenue_cat",
            "Название должности": "job_title",
            "Регион/область (заполняется автоматически)": "region",
            "Грейд / Уровень обзора": "grade",
            "Код функции": "function",
            "Код подфункции": "subfunction",
            "Код специализации": "spec",
            "Базовый оклад (BP)": "BP",
            "Индустрия": "industry",
            "Количество сотрудников категория": "headcount_cat",
            "Выручка категория": "revenue_cat",
            "Уровень подчинения по отношению к Первому лицу компании": "n_level",
        },
        inplace=True,
    )

    df["code"] = df.apply(
        lambda x: x["spec"] if not (is_empty_value(x["spec"])) else x["subfunction"],
        axis=1,
    )
    df = calculate_n_level(df)
    # df = normalize(df, ['company', 'region', 'code'], 'BP_boxcox', 'Scaled_BP_Code')
    df = add_salary_to_headcount_ratio(df, ["company", "region"], "STH_C")
    df = add_salary_to_headcount_ratio(df, ["company", "code", "region"], "STH_C_R")
    df = add_company_salary_sum(df)
    df["EmpBP_Portion_C"] = df["BP"] / df["salary_sum_comp"]

    df = log_function(df, "EmpBP_Portion_C", "Logged_EmpBP_Portion_C")
    df = normalize(df, ["company"], "Logged_EmpBP_Portion_C", "Scaled_EmpBP_Portion_C")

    grouped = (
        df.groupby(["company", "function"])
        .agg(subfunc_num=("subfunction", lambda x: len(pd.unique(x))))
        .reset_index()
    )
    df = df.merge(grouped, on=["company", "function"], how="left")

    median = (
        df.groupby(["company", "code", "region"])
        .agg(median_sp_r=("BP", "median"))
        .reset_index()
    )
    df = df.merge(median, on=["company", "code", "region"], how="left")

    df["CR_SP_R"] = df["BP"] / df["median_sp_r"]
    df = log_function(df, "CR_SP_R", "Logged_CR_SP_R")
    df = normalize(
        df,
        [
            "company",
            "region",
            "code",
        ],
        "Logged_CR_SP_R",
        "Scaled_CR_SP_R",
    )

    df = df.drop(
        [
            "EmpBP_Portion_C",
            "Logged_CR_SP_R",
            "CR_SP_R",
            "Logged_EmpBP_Portion_C",
            "BP",
        ],
        axis=1,
    )

    print("Размер датасета: ", df.shape[0])
    # sub = pd.read_excel('data_model_2/feats_2_5000_new_rows (1).xlsx', index_col=None)
    # sub2 = pd.read_excel('data_model_2/feats_2_1000_from_grades.xlsx', index_col=None)
    # df = pd.concat([df, sub, sub2])
    print("Размер датасета после добавления новых данных: ", df.shape[0])
    cols = ["job_title", "industry", "region", "headcount_cat", "revenue_cat", "code"]
    bad = {"", "-", "none", "nan", "null"}

    norm = df[cols].astype(str).apply(lambda s: s.str.strip().str.lower())
    bad_mask = norm.isin(bad) | df[cols].isna()
    to_drop = bad_mask.any(axis=1)
    df = df[~to_drop]
    print(
        "Размер датасета после удаления строк с пустыми значениями в обязательных колонках: ",
        df.shape[0],
    )
    df["job_title_cleaned"] = df["job_title"].apply(sanitize_text)
    df["industry_cleaned"] = df["industry"].apply(sanitize_text)
    df["region_cleaned"] = df["region"].apply(sanitize_text)
    df["headcount_cat_cleaned"] = df["headcount_cat"].apply(sanitize_text)
    df["revenue_cat_cleaned"] = df["revenue_cat"].apply(sanitize_text)
    df["code_cleaned"] = df["code"].astype(str).str.strip().str.upper()
    df["subfunction_cleaned"] = df["subfunction"].astype(str).str.strip().str.upper()
    df["n_level_cleaned"] = df["n_level"].map(
        lambda value: "" if is_empty_value(value) else str(value).strip()
    )
    lead_mask = get_lead_mask(df)
    df = df[lead_mask].copy()

    return df
