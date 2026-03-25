import json
import re
import warnings
from datetime import datetime
from typing import Iterable

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from modules import LP
from modules.module_4.grade_utils import calculate_f_new
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import PowerTransformer
from tqdm import tqdm

tqdm.pandas(desc="Processing")
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    message=".*DataFrameGroupBy.apply operated on the grouping columns.*",
)
# warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*invalid value encountered in log*")

TEAMLEAD_STEMS: set[str] = {
    "управляющ",
    "teamlead",
    "lead",
    "начальник",
    "руковод",
    "директор",
    "президент",
}
TEAMLEAD_PHRASES: tuple[str, ...] = ("team lead",)
SENIOR_STEMS: set[str] = {"ведущ", "глав", "senior", "прораб"}
MIDDLE_STEMS: set[str] = {"старш", "mid", "middle"}
JUNIOR_STEMS: set[str] = {"junior", "jr", "младш", "начинающ"}


MODEL_CATEGORICAL_FEATURES: tuple[str, ...] = (
    "code",
    "industry_cleaned",
    "region_cleaned",
    "headcount_cat_cleaned",
    "revenue_cat_cleaned",
    "seniority",
)
MODEL_NUMERICAL_FEATURES: tuple[str, ...] = (
    "Scaled_BP",
    "Scaled_BP_Region",
    "Scaled_BP_Code",
    "Scaled_BP_rcs",
    "STH_C",
    "STH_SP",
    "STH_C_R",
    "Scaled_EmpBP_Portion_C",
    "Scaled_CR_SP_R",
    "subfunc_num",
)


class Dataset:
    def __init__(self, df=None):
        self.df = df

    def _match_stems(self, tokens: Iterable[str], stems: set[str]) -> bool:
        return any(tok.startswith(stem) for tok in tokens for stem in stems)

    def _categorize_title(self, title: str | None) -> int:
        if not title or not isinstance(title, str):
            return 2

        normalized = re.sub(r"[^\w\s]", " ", title.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        tokens = normalized.split()

        if any(phrase in normalized for phrase in TEAMLEAD_PHRASES):
            return 4
        if self._match_stems(tokens, TEAMLEAD_STEMS):
            return 4
        if self._match_stems(tokens, SENIOR_STEMS):
            return 3
        if self._match_stems(tokens, JUNIOR_STEMS):
            return 1
        return 2

    def _process(self, row):
        bad = ["none", "nan", "-", ""]
        if str(row["Код специализации"]).strip().lower() not in bad:
            return row["Код специализации"]
        else:
            return row["Код подфункции"]

    def prepare_features(self, df: pd.DataFrame, train):
        df = df.copy()
        if "__row_id__" not in df.columns:
            df["__row_id__"] = np.arange(len(df))
        if train:
            pt = PowerTransformer(method="box-cox", standardize=True)
            df["BP_boxcox"] = pt.fit_transform(df[["Базовый оклад (BP)"]])
            joblib.dump(
                pt, "modules/module_4/grade_model_weights/bp_boxcox_transformer.pkl"
            )
            df["seniority"] = df["job_title"].progress_apply(
                lambda x: self._categorize_title(x)
            )
        else:
            pt = joblib.load(
                "modules/module_4/grade_model_weights/bp_boxcox_transformer.pkl"
            )
            df["BP_boxcox"] = pt.transform(df[["Базовый оклад (BP)"]])
            df["code"] = df.progress_apply(lambda x: self._process(x), axis=1)
            df["seniority"] = df["Название должности"].progress_apply(
                lambda x: self._categorize_title(x)
            )

        df["Базовый оклад (BP)"] = df["BP_boxcox"]
        print(f"Lambda Box-Cox: {pt.lambdas_[0]}")

        if train:
            df = df.drop_duplicates()
        print("Calculating features...")
        df = calculate_f_new(df)
        df[list(MODEL_CATEGORICAL_FEATURES)] = (
            df[list(MODEL_CATEGORICAL_FEATURES)].fillna("missing").astype(str)
        )

        row_ids = df["__row_id__"].astype(int) if "__row_id__" in df.columns else None
        df_cat = df[list(MODEL_CATEGORICAL_FEATURES)]
        df_num = df[list(MODEL_NUMERICAL_FEATURES)].astype(float)
        X = pd.concat([df_cat, df_num], axis=1)
        if row_ids is not None:
            X.index = row_ids.to_numpy()

        cat_feature_indices = [
            X.columns.get_loc(col) for col in MODEL_CATEGORICAL_FEATURES
        ]

        y = df["grade"]
        if row_ids is not None:
            y.index = row_ids.to_numpy()
        return X, cat_feature_indices, y


class GradePredictor:
    def __init__(self, iterations: int = 5000, path_to_model=None):
        self.iterations = iterations

        if path_to_model:
            loaded = CatBoostRegressor()
            loaded.load_model(path_to_model)
            self.model = loaded
        else:
            self.model = CatBoostRegressor(
                loss_function="Quantile:alpha=0.6",
                eval_metric="Quantile:alpha=0.6",
                depth=10,
                iterations=self.iterations,
                learning_rate=0.03,
                random_seed=42,
                task_type="GPU",
                od_type="Iter",
                od_wait=50,
                verbose=True,
            )

    def train(self, df):
        df = df.copy()
        data_preparer = Dataset(df)

        print("Preparing features...")
        X, cat_idx, y = data_preparer.prepare_features(df, train=True)
        self.cat_features = cat_idx

        print("Train-test split...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.1, random_state=42
        )

        self.X_test = X_test
        self.y_test = y_test
        self.data = df

        self.model.fit(
            X_train,
            y_train,
            eval_set=(X_test, y_test),
            cat_features=self.cat_features,
            verbose=True,
            use_best_model=True,
        )

        base_name = "models/model"
        ext = ".cbm"

        timestamp = datetime.now().strftime("%d_%m_%H_%M")
        filename = f"{base_name}_{timestamp}{ext}"

        self.model.save_model(filename)
        print(f"Model saved in {filename}")
        return X_test

    def _attach_predictions(self, df, idx_test, preds):
        out = df.copy()
        out[LP.grade] = "-"

        if len(idx_test) > 0:
            rounded = np.floor(np.asarray(preds, dtype=float) + 0.5).astype(int)
            row_pos = np.asarray(idx_test, dtype=int)
            grade_col_pos = out.columns.get_loc(LP.grade)
            out.iloc[row_pos, grade_col_pos] = rounded

        with open(
            "modules/module_4/grade_model_weights/codes.json", "r", encoding="utf-8"
        ) as f:
            codes = json.load(f)

        data = Dataset()
        out["code"] = out.progress_apply(lambda x: data._process(row=x), axis=1)
        out["description"] = out["code"].apply(
            lambda x: codes.get(str(x), {}).get("description", "-")
        )
        out.loc[out[LP.grade] == "-", "description"] = "-"
        out = out.drop(columns=["code"])
        return out

    def predict(self, df):
        if df.empty:
            return self._attach_predictions(
                df, pd.Index([], dtype=int), np.array([], dtype=float)
            )

        df_for_pred = df.copy()
        df_for_pred["__row_id__"] = np.arange(len(df_for_pred))
        prepare = Dataset(df_for_pred)
        print("Preparing features...")
        X, _, _ = prepare.prepare_features(df_for_pred, train=False)

        if X.empty:
            print("No valid rows for model prediction. Returning placeholders.")
            preds = np.array([], dtype=float)
            idx_test = pd.Index([], dtype=int)
        else:
            print("Predicting...")
            preds = self.model.predict(X)
            idx_test = X.index

        print("Mapping predictions...")
        df_preds = self._attach_predictions(df, idx_test, preds)
        return df_preds
