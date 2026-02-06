import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.cell.cell import MergedCell
from copy import copy


def _norm(value):
    return str(value).strip()


def _get_sheet(wb, target_name):
    for s in wb.sheetnames:
        if s.strip() == target_name:
            return wb[s]
    raise ValueError(f'Sheet "{target_name}" not found')


def _build_header_map(ws, header_row):
    header_map = {}
    for c in range(1, ws.max_column + 1):
        cell = ws.cell(row=header_row, column=c)
        if isinstance(cell, MergedCell):
            continue
        if cell.value is None:
            continue
        header_map[_norm(cell.value)] = c
    return header_map


def _clear_values_below(ws, header_row):
    for row in ws.iter_rows(min_row=header_row + 1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in row:
            if isinstance(cell, MergedCell):
                continue
            cell.value = None


def _write_common_columns(ws, df, header_row, header_map):
    for name, col_idx in header_map.items():
        cell = ws.cell(row=header_row, column=col_idx)
        if isinstance(cell, MergedCell):
            continue
        cell.value = name

    for r_idx, row in enumerate(df.itertuples(index=False, name=None), start=header_row + 1):
        for name, value in zip(df.columns, row):
            col_idx = header_map.get(_norm(name))
            if not col_idx:
                continue
            cell = ws.cell(row=r_idx, column=col_idx)
            if isinstance(cell, MergedCell):
                continue
            cell.value = value


def _write_full(ws, df, header_row):
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), start=header_row):
        for c_idx, value in enumerate(row, start=1):
            cell = ws.cell(row=r_idx, column=c_idx)
            if isinstance(cell, MergedCell):
                continue
            cell.value = value


def _extend_styles(ws, header_row, last_data_row):
    if last_data_row <= ws.max_row:
        return

    style_src_row = header_row + 1 if (header_row + 1) <= ws.max_row else header_row
    for r in range(ws.max_row + 1, last_data_row + 1):
        for c in range(1, ws.max_column + 1):
            src = ws.cell(row=style_src_row, column=c)
            dst = ws.cell(row=r, column=c)
            if src.has_style:
                dst._style = copy(src._style)
            if src.hyperlink:
                dst._hyperlink = copy(src.hyperlink)
            if src.comment:
                dst.comment = copy(src.comment)


def write_df_with_template(
    df,
    template_path,
    out_path,
    sheet_name='Данные',
    header_row=7,
    only_common_columns=True,
):
    df = df.copy()
    df.columns = df.columns.map(str)

    wb = load_workbook(template_path)
    ws = _get_sheet(wb, sheet_name)

    header_map = _build_header_map(ws, header_row)
    _clear_values_below(ws, header_row)

    if only_common_columns:
        common_cols = [c for c in df.columns if _norm(c) in header_map]
        df = df[common_cols]
        _write_common_columns(ws, df, header_row, header_map)
    else:
        _write_full(ws, df, header_row)

    last_data_row = header_row + df.shape[0]
    _extend_styles(ws, header_row, last_data_row)

    wb.save(out_path)
