# All the variables are imported from LP.py file
import os
import re
import sys
import warnings

import numpy as np
import pandas as pd

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


def module_3(input_folder, output_folder, params=None):
    # save_to_parquet = params["save_to_parquet"]
    counter = 0
    res_df = pd.DataFrame()
    res_lower_mrot_df = pd.DataFrame()
    res_high_ti = pd.DataFrame()
    print("Module 3: Compensating Elements.")

    for file in os.listdir(input_folder):
        # Check if the file is an Excel file
        if file.endswith(".xlsx") or file.endswith(".xls") or file.endswith(".xlsm"):
            counter += 1
            # errors = [], # Список ошибок

            print(f"Processing file {counter}: {file}")
            # Process the Excel file
            file_path = os.path.join(input_folder, file)

            # Exporting the dataframe from an excel file
            # For SDFs
            try:
                df = pd.read_excel(file_path, sheet_name="Total Data", index_col=0)
            except Exception:
                try:
                    df = pd.read_excel(file_path, sheet_name="Данные", header=6)
                    df_company = pd.read_excel(
                        file_path, sheet_name=LP.company_data, header=1
                    )
                    df[LP.company_name] = df_company.iloc[0, 3]
                    df[LP.gi_company_name] = df_company.iloc[0, 3]
                    df[LP.gi_sector] = df_company.iloc[1, 3]
                    df[LP.gi_origin] = df_company.iloc[2, 3]
                    df[LP.gi_headcount_cat] = df_company.iloc[3, 3]
                    df[LP.gi_revenue_cat] = df_company.iloc[4, 3]
                    df[LP.gi_contact_name] = df_company.iloc[5, 3]
                    df[LP.gi_title] = df_company.iloc[6, 3]
                    df[LP.gi_tel] = df_company.iloc[7, 3]
                    df[LP.gi_email] = df_company.iloc[8, 3]
                except Exception as e:
                    print(f"Error reading file '{file_path}': {e}")
                    continue
            # df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=0)
            # print(df.keys())

            # Apply cleaning to column names
            df.columns = [
                re.sub(
                    r"\s+", " ", str(col).replace("\n", " ").replace("\r", " ")
                ).strip()
                for col in df.columns
            ]

            ultimate_df = calculate_compensation_elements(
                df,
                # Input columns
                monthly_salary=LP.monthly_salary,
                salary_rate=LP.salary_rate,
                number_annual_salaries=LP.number_monthly_salaries,
                additional_pay=LP.additional_pay,
                sti_eligibility=LP.sti_eligibility,
                tenure=LP.tenure,
                fact_sti=LP.fact_sti,
                target_sti=LP.target_sti,
                fact_lti=LP.fact_lti,
                target_lti_per=LP.target_lti_per,
                lti_eligibility=LP.lti_eligibility,
                # Output columns
                annual_salary=LP.annual_salary,
                base_pay=LP.base_pay,
                fact_sti_out=LP.fact_sti_out,
                fact_sti_out_alt=LP.fact_sti_out_alt,
                target_sti_out=LP.target_sti_out,
                tc_pay=LP.tc_pay,
                ltip_pay=LP.ltip_pay,
                ltip_pay_alt=LP.ltip_pay_alt,
                ttc_pay=LP.ttc_pay,
                tltip_pay=LP.tltip_pay,
                tdc_pay=LP.tdc_pay,
                ttdc_pay=LP.ttdc_pay,
                # Constants
                positive_v=LP.positive_v,
                negative_v=LP.negative_v,
                tenure_value=LP.tenure_value,
                fact_sti_threshold=0.05,
            )

            # Fixed version 1: Fact LTI to TC comparison
            ultimate_df = fact_lti_to_tc(
                ultimate_df,
                lti_col=LP.fact_lti,
                tc_col=LP.tc_pay,
                output_col="Fact LTI < TC",
            )

            # Fixed version 2: Target LTI to TTC comparison
            ultimate_df = target_lti_to_ttc(
                ultimate_df,
                lti_col=LP.tltip_pay,
                ttc_col=LP.ttc_pay,
                base_pay_col=LP.base_pay,
                output_col="Target LTI < TTC",
            )

            ultimate_df = lower_mrot(ultimate_df, LP.tc_pay, "TC > MROT")
            ultimate_df = lower_mrot(ultimate_df, LP.ttc_pay, "TTC > MROT")

            # Get rows where TC OR TTC is below MROT
            low_tc_mask = ~ultimate_df["TC > MROT"]
            low_ttc_mask = ~ultimate_df["TTC > MROT"]
            lower_mrot_df = ultimate_df[low_tc_mask | low_ttc_mask].copy()

            ultimate_df = ti_higher(ultimate_df, LP.target_sti_out, "Normal TI", 3)
            high_ti = ultimate_df[~ultimate_df["Normal TI"]]

            ultimate_df = lti_checks(
                ultimate_df,
                LP.fact_lti,
                LP.fact_lti_1,
                LP.fact_lti_2,
                LP.fact_lti_3,
                "Fact LTI = Fact LTI Parts",
            )
            ultimate_df = lti_checks(
                ultimate_df,
                LP.target_lti_per,
                LP.target_lti_1,
                LP.target_lti_2,
                LP.target_lti_3,
                "Target LTI = Target LTI Parts",
            )
            ultimate_df["LTI & grade >= 13"] = ultimate_df.apply(
                lambda x: not (x[LP.grade] < 13 and (not str(x[LP.fact_lti]) == "nan")),
                axis=1,
            )

            ultimate_df["EMA & grade >= 17"] = ultimate_df.apply(
                lambda x: (
                    not ((x[LP.grade] < 17) and (x[LP.subfunction_code] == "EMA"))
                ),
                axis=1,
            )
            ultimate_df["PRB & grade <= 14"] = ultimate_df.apply(
                lambda x: (
                    not ((x[LP.grade] > 14) and (x[LP.subfunction_code] == "PRB"))
                ),
                axis=1,
            )
            ultimate_df["XXZ & grade >= 14"] = ultimate_df.apply(
                lambda x: (
                    not ((x[LP.grade] < 14) and (x[LP.subfunction_code][2] == "Z"))
                ),
                axis=1,
            )

            # ultimate_df = validate_compensation_ranges(
            #     ultimate_df,
            #     comp_column=LP.base_pay,
            #     grade_col=LP.grade,
            #     output_col="BP_within_range",
            #     comp_type="BP",
            # )

            # ultimate_df = validate_compensation_ranges(
            #     ultimate_df,
            #     comp_column=LP.tc_pay,
            #     grade_col=LP.grade,
            #     output_col="TC_within_range",
            #     comp_type="TC",
            # )

            # ultimate_df = validate_compensation_ranges(
            #     ultimate_df,
            #     comp_column=LP.ttc_pay,
            #     grade_col=LP.grade,
            #     output_col="TTC_within_range",
            #     comp_type="TTC",
            # )

            # Make sure to delete whitespaces from
            ultimate_df[LP.gi_origin] = ultimate_df[LP.gi_origin].where(
                ultimate_df[LP.gi_origin].isna(),  # keep NaNs as-is
                ultimate_df[LP.gi_origin].astype(str).str.strip(),
            )

            res_df = pd.concat([res_df, ultimate_df])
            res_lower_mrot_df = pd.concat([res_lower_mrot_df, lower_mrot_df])
            res_high_ti = pd.concat([res_high_ti, high_ti])

            try:
                output_path = os.path.join(output_folder, file)
                res_df = res_df.loc[:, ~res_df.columns.str.contains("^Unnamed")]
                res_lower_mrot_df = res_lower_mrot_df.loc[
                    :, ~res_lower_mrot_df.columns.str.contains("^Unnamed")
                ]
                res_high_ti = res_high_ti.loc[
                    :, ~res_high_ti.columns.str.contains("^Unnamed")
                ]
                with pd.ExcelWriter(output_path) as writer:
                    res_df.to_excel(writer, index=True, sheet_name="Total Data")
                    res_lower_mrot_df.to_excel(
                        writer, index=True, sheet_name="Lower than MROT"
                    )
                    res_high_ti.to_excel(writer, index=True, sheet_name="High TI")
                print(f"Successfully saved Excel file to: {output_path}")
            except Exception as e:
                print(f"Failed to save Excel file: {e}")

        # if save_to_parquet:
        #     save_db_to_parquet(ultimate_df, output_folder)
        print("-------------------------")
        # if company_files:
        #     process_with_past_year(company_files, df)


# Function to compare how realistic is the certain compensation element
def validate_compensation_ranges(
    df, comp_column, grade_col, output_col, comp_type="BP"
):
    # Get input data
    compensation_values = df[comp_column].copy()
    grades = pd.to_numeric(df[grade_col], errors="coerce")

    # Initialize result with False
    result = pd.Series(False, index=df.index, dtype=bool)

    # Handle NaN compensation values - set to True
    nan_comp_mask = compensation_values.isna()
    result.loc[nan_comp_mask] = True

    # Create lookup series for min and max values
    grade_min_map = {}
    grade_max_map = {}

    for grade, comp_data in LP.crazy_ranges.items():
        if comp_type in comp_data:
            grade_min_map[grade] = comp_data[comp_type]["min"]
            grade_max_map[grade] = comp_data[comp_type]["max"]

    # Convert to pandas Series for vectorized operations
    min_values = grades.map(grade_min_map)
    max_values = grades.map(grade_max_map)

    # Vectorized comparison: compensation is within range
    valid_grade_mask = grades.isin(LP.crazy_ranges.keys())
    within_range_mask = (
        valid_grade_mask
        & ~nan_comp_mask
        & (compensation_values >= min_values)
        & (compensation_values <= max_values)
    )

    # Set True for values within range
    result.loc[within_range_mask] = True

    # Create output column
    df[output_col] = result

    return df


def lti_checks(df, main_lti, lti_1, lti_2, lti_3, output_col):
    df[output_col] = (df[main_lti] == (df[lti_1] + df[lti_2] + df[lti_3])) | df[
        main_lti
    ].isna()
    return df


def ti_higher(df, input_col, output_col, threshold=3):
    df[output_col] = (df[input_col] < threshold) | (df[input_col].isna())
    return df


def lower_mrot(df, input_col, output_col, mrot_value=268_800):
    """
    Compare column values against MROT threshold.

    Parameters:
    df (pd.DataFrame): Input dataframe
    input_col (str): Column name to compare against MROT
    output_col (str): Output column name for results
    mrot_value (float): MROT threshold value (default: 268,800)

    Returns:
    pd.DataFrame: DataFrame with new comparison column
    """
    df[output_col] = (df[input_col] > mrot_value) | (df[input_col].isna())
    return df


def fact_lti_to_tc(df, lti_col, tc_col, output_col):
    """
    Compare fact LTI to TC values.
    Returns True if LTI < TC, or if either value is NaN.

    Parameters:
    df (pd.DataFrame): Input dataframe
    lti_col (str): Column name for LTI values
    tc_col (str): Column name for TC values
    output_col (str): Output column name

    Returns:
    pd.DataFrame: Modified dataframe with comparison results
    """
    # Fixed: Compare df[lti_col] < df[tc_col], not df[lti_col] < tc_col
    # Fixed: tc_ol -> tc_col typo
    df[output_col] = (
        (df[lti_col] < df[tc_col]) | (df[tc_col].isna()) | (df[lti_col].isna())
    )
    return df


def target_lti_to_ttc(df, lti_col, ttc_col, base_pay_col, output_col):
    """
    Compare target LTI to TTC values.
    Returns True if (LTI * base_pay) < TTC, or if either value is NaN.

    Parameters:
    df (pd.DataFrame): Input dataframe
    lti_col (str): Column name for LTI values
    ttc_col (str): Column name for TTC values
    base_pay_col (str): Column name for base pay values
    output_col (str): Output column name

    Returns:
    pd.DataFrame: Modified dataframe with comparison results
    """
    # Fixed: Use df[ttc_col] instead of ttc_pay variable
    # Fixed: ttc_ol -> ttc_col typo
    # Fixed: Add base_pay_col parameter instead of hardcoded base_pay
    df[output_col] = (
        ((df[lti_col] * df[base_pay_col]) < df[ttc_col])
        | (df[ttc_col].isna())
        | (df[lti_col].isna())
    )
    return df


# Fucntion to calculate compensation elements
def calculate_compensation_elements(
    df,
    # Input column names
    monthly_salary,
    salary_rate,
    number_annual_salaries,
    additional_pay,
    sti_eligibility,
    tenure,
    fact_sti,
    target_sti,
    fact_lti,
    target_lti_per,
    lti_eligibility,
    # Output column names
    annual_salary,
    base_pay,
    fact_sti_out,
    fact_sti_out_alt,
    target_sti_out,
    tc_pay,
    ltip_pay,
    ltip_pay_alt,
    ttc_pay,
    tltip_pay,
    tdc_pay,
    ttdc_pay,
    # Constants/thresholds
    positive_v,
    negative_v,
    tenure_value,
    fact_sti_threshold,
):
    """
    Vectorized function to calculate various compensation elements.

    Parameters:
    df (pd.DataFrame): Input dataframe

    Input column names (str):
    - monthly_salary: Monthly salary column
    - salary_rate: Salary rate column
    - number_annual_salaries: Number of annual salaries column
    - additional_pay: Additional pay column
    - sti_eligibility: STI eligibility column
    - tenure: Tenure column
    - fact_sti: Actual STI column
    - target_sti: Target STI column
    - fact_lti: Actual LTI column
    - target_lti_per: Target LTI percentage column
    - lti_eligibility: LTI eligibility column

    Output column names (str):
    - annual_salary: Annual salary output column
    - base_pay: Base pay output column
    - fact_sti_out: Filtered actual STI output column
    - fact_sti_out_alt: Alternative actual STI output column
    - target_sti_out: Target STI output column
    - tc_pay: Total cash pay column
    - ltip_pay: LTIP pay column
    - ltip_pay_alt: Alternative LTIP pay column
    - ttc_pay: Target total cash pay column
    - tltip_pay: Target LTIP pay column
    - tdc_pay: Total direct compensation column
    - ttdc_pay: Target total direct compensation column

    Constants:
    - positive_v: Value indicating positive eligibility
    - negative_v: Value indicating negative eligibility
    - tenure_value: Tenure value for exclusion
    - fact_sti_threshold: Threshold for STI validation

    Returns:
    pd.DataFrame: Modified dataframe with calculated compensation elements
    """

    def _to_numeric(series):
        # Handles common Excel text formats like "1 234,56"
        return pd.to_numeric(
            series.astype(str)
            .str.replace("\xa0", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace(",", ".", regex=False),
            errors="coerce",
        )

    numeric_columns = [
        monthly_salary,
        salary_rate,
        number_annual_salaries,
        additional_pay,
        LP.region_coeff,
        fact_sti,
        target_sti,
        fact_lti,
        target_lti_per,
    ]

    for col in numeric_columns:
        df[col] = _to_numeric(df[col])

    # Annual salary calculation
    df[annual_salary] = (
        df[monthly_salary] / df[salary_rate] * df[number_annual_salaries]
    )

    # Base pay calculation
    # BP = (MS/SR*NS) + ADD + (RC/SR*NS)
    # Where MS=Month Salary, SR=Salary Rate, NS=Number salaries, ADD=Additional Pay, RC=Region Coefficient
    df[base_pay] = (
        df[annual_salary]
        + (
            np.where(df[additional_pay].isnull(), 0, df[additional_pay])
            / df[salary_rate]
        )
        + (
            np.where(df[LP.region_coeff].isnull(), 0, df[LP.region_coeff])
            * df[number_annual_salaries]
            / df[salary_rate]
        )
    )

    # Actual STI output (with threshold filter)
    df[fact_sti_out] = np.where(
        (df[sti_eligibility] == negative_v) | (df[tenure] == tenure_value),
        np.nan,
        np.where(
            (df[sti_eligibility] == positive_v)
            & (df[fact_sti].notnull())
            & ((df[fact_sti] / df[base_pay]) > fact_sti_threshold),
            df[fact_sti],
            np.nan,
        ),
    )

    # Actual STI output alternative (without threshold filter)
    df[fact_sti_out_alt] = np.where(
        (df[sti_eligibility] == negative_v) | (df[tenure] == tenure_value),
        np.nan,
        np.where(
            (df[sti_eligibility] == positive_v) & (df[fact_sti].notnull()),
            df[fact_sti],
            np.nan,
        ),
    )

    df[target_sti] = pd.to_numeric(df[target_sti], errors="coerce")

    # Target STI output
    df[target_sti_out] = np.where(
        (df[sti_eligibility] == positive_v)
        & (df[target_sti].notnull())
        & (df[target_sti] != 0),
        df[target_sti],
        np.nan,
    )

    # Total cash pay
    df[tc_pay] = np.where(
        df[sti_eligibility] == negative_v,
        df[base_pay],
        np.where(
            (df[sti_eligibility] == positive_v)
            & (df[fact_sti].notnull())
            & (df[tenure] != tenure_value),
            df[base_pay] + df[fact_sti_out_alt],
            np.nan,
        ),
    )

    # LTIP pay (with threshold filter)
    df[ltip_pay] = np.where(
        (df[lti_eligibility] == negative_v) | (df[tenure] == tenure_value),
        np.nan,
        np.where(
            (df[lti_eligibility] == positive_v)
            & (df[fact_lti].notnull())
            & ((df[fact_lti] / df[base_pay]) > fact_sti_threshold),
            df[fact_lti],
            np.nan,
        ),
    )

    # LTIP pay alternative (without threshold filter)
    df[ltip_pay_alt] = np.where(
        (df[lti_eligibility] == negative_v) | (df[tenure] == tenure_value),
        np.nan,
        np.where(
            (df[lti_eligibility] == positive_v) & (df[fact_lti].notnull()),
            df[fact_lti],
            np.nan,
        ),
    )

    # Target total cash pay
    df[ttc_pay] = np.where(
        df[sti_eligibility] == negative_v,
        df[base_pay],
        np.where(
            (df[sti_eligibility] == positive_v)
            & (df[target_sti].notnull())
            & (df[target_sti] != 0),
            df[base_pay] * (1 + df[target_sti]),
            np.nan,
        ),
    )

    # Target LTIP pay
    df[tltip_pay] = np.where(
        (df[lti_eligibility] == positive_v)
        & (df[target_lti_per].notnull())
        & (df[target_lti_per] != 0),
        df[target_lti_per],
        np.nan,
    )

    # Total direct compensation
    df[tdc_pay] = np.where(
        (df[sti_eligibility] == positive_v) & (df[fact_sti].isna())
        | (df[lti_eligibility] == positive_v) & (df[fact_lti].isna())
        | (df[tenure] == tenure_value)
        & ((df[sti_eligibility] == positive_v) | (df[lti_eligibility] == positive_v)),
        np.nan,
        np.where(
            (df[sti_eligibility].isna()) | (df[lti_eligibility].isna()),
            np.nan,
            df[base_pay]
            + np.where(
                (df[sti_eligibility] == negative_v) | (df[tenure] == tenure_value),
                0,
                np.where(
                    (df[sti_eligibility] == positive_v) & df[fact_sti].isna(),
                    0,
                    df[fact_sti],
                ),
            )
            + np.where(
                (df[lti_eligibility] == negative_v) | (df[tenure] == tenure_value),
                0,
                np.where(
                    (df[lti_eligibility] == positive_v) & df[fact_lti].isna(),
                    0,
                    df[fact_lti],
                ),
            ),
        ),
    )

    # Target total direct compensation
    df[ttdc_pay] = np.where(
        (df[sti_eligibility] == positive_v)
        & (df[target_sti].isna() | (df[target_sti] == 0))
        | (df[lti_eligibility] == positive_v)
        & (df[target_lti_per].isna() | (df[target_lti_per] == 0)),
        np.nan,
        df[base_pay]
        + np.where(df[sti_eligibility] == positive_v, df[base_pay] * df[target_sti], 0)
        + np.where(
            df[lti_eligibility] == positive_v, df[base_pay] * df[target_lti_per], 0
        ),
    )

    # Clean up temporary columns
    df.drop([fact_sti_out_alt, ltip_pay_alt], axis=1, inplace=True)

    return df
