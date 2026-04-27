from __future__ import annotations

import numpy as np
import pandas as pd
from models.model_1.model import SubPredictor
from models.model_2.model import LeadPredictor
from models.model_2.utils import get_lead_mask

TARGET_COL = "Грейд / Уровень обзора"
PREDICTION_COL = "predicted_grade"
ROW_ID_COL = "__grade_predictor_row_id__"


class GradePredictor:
    def __init__(
        self,
        sub_predictor: SubPredictor | None = None,
        lead_predictor: LeadPredictor | None = None,
        sub_predictor_kwargs: dict | None = None,
        lead_predictor_kwargs: dict | None = None,
    ):
        self.sub_predictor = sub_predictor or SubPredictor(
            **(sub_predictor_kwargs or {})
        )
        self.lead_predictor = lead_predictor or LeadPredictor(
            **(lead_predictor_kwargs or {})
        )

    def _split_df(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
        lead_mask = get_lead_mask(df)
        sub_df = df.loc[~lead_mask].copy()
        return sub_df, lead_mask

    def _merge_predictions(
        self,
        result: pd.DataFrame,
        predicted_df: pd.DataFrame,
        row_id_col: str,
    ) -> None:
        if predicted_df.empty:
            return

        valid_predicted_df = predicted_df[predicted_df[PREDICTION_COL].notna()].copy()
        if valid_predicted_df.empty:
            return

        row_ids = valid_predicted_df[row_id_col].to_numpy(dtype=int)
        values = valid_predicted_df[PREDICTION_COL].to_numpy()
        result.iloc[row_ids, result.columns.get_loc(PREDICTION_COL)] = values

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        row_id_col = ROW_ID_COL
        while row_id_col in df.columns:
            row_id_col = f"_{row_id_col}_"

        work_df = df.copy()
        work_df[row_id_col] = np.arange(len(work_df))

        result = work_df.copy()
        result[PREDICTION_COL] = pd.NA

        sub_df, lead_mask = self._split_df(work_df)

        if not sub_df.empty:
            sub_predicted = self.sub_predictor.predict(sub_df)
            self._merge_predictions(result, sub_predicted, row_id_col)

        if lead_mask.any():
            lead_predicted = self.lead_predictor.predict(work_df)
            self._merge_predictions(
                result, lead_predicted.loc[lead_mask].copy(), row_id_col
            )

        result[TARGET_COL] = result[PREDICTION_COL]
        return result.drop(columns=[row_id_col, PREDICTION_COL])
