import re
import warnings
from pathlib import Path
from typing import Iterable

import joblib
import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import PowerTransformer
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent
from models.model_2.utils import calculate_f_new  # noqa: E402

tqdm.pandas(desc="Processing")

SOURCE_ROW_ID_COL = "__source_row_id__"
TARGET_COL = "Грейд / Уровень обзора"
PREDICTION_COL = "predicted_grade"
BASE_SALARY_COL = "Базовый оклад (BP)"
JOB_TITLE_COL = "Название должности"
MODEL_FILENAME = "model.cbm"
TRANSFORMER_FILENAME = "bp_boxcox_transformer.pkl"

MANAGER_STEMS: set[str] = {"начальник", "руководитель", "менеджер", "управляющ"}

TEAMLEAD_STEMS: set[str] = {"teamlead", "lead", "тимлид", "техлид"}

TEAMLEAD_PHRASES: tuple[str, ...] = (
    "team lead",
    "tech lead",
    "технический лидер",
)

HEAD_OF_PHRASES: tuple[str, ...] = (
    "директор департамента",
    "начальник департамента",
    "head of",
    "руководитель департамента",
    "заместитель генерального директора по",
    "вице президент",
    "руководитель направления",
    "руководитель бизнес-направления",
)

CEO_STEMS: set[str] = {"генеральный директор"}


def _compute_macro_f1(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    labels = np.unique(np.concatenate([y_true, y_pred]))
    if not len(labels):
        return float("nan")

    f1_scores = []
    for label in labels:
        tp = np.sum((y_true == label) & (y_pred == label))
        fp = np.sum((y_true != label) & (y_pred == label))
        fn = np.sum((y_true == label) & (y_pred != label))

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if (precision + recall)
            else 0.0
        )
        f1_scores.append(f1)

    return float(np.mean(f1_scores))


class Dataset:
    def __init__(self, df=None, artifacts_dir=None):
        self.df = df
        self.artifacts_dir = (
            Path(artifacts_dir) if artifacts_dir else BASE_DIR / "grade_model_weights"
        )

    def _normalize_title_for_match(self, text: str | None) -> str:
        if not text or not isinstance(text, str):
            return ""
        normalized = re.sub(r"[^\w\s]", " ", text.lower())
        return re.sub(r"\s+", " ", normalized).strip()

    def _match_stems(
        self, tokens: Iterable[str], stems: set[str] | tuple[str, ...]
    ) -> bool:
        return any(tok.startswith(stem) for tok in tokens for stem in stems)

    def _contains_phrases(self, normalized_text: str, phrases: Iterable[str]) -> bool:
        return any(
            self._normalize_title_for_match(phrase) in normalized_text
            for phrase in phrases
        )

    def _categorize_title(
        self,
        title: str | None,
        function_code: str | None = None,
        specialization_code: str | None = None,
    ) -> str:
        normalized = self._normalize_title_for_match(title)
        tokens = normalized.split()
        function_code_normalized = (
            str(function_code).strip().upper() if function_code is not None else ""
        )
        specialization_code_normalized = (
            str(specialization_code).strip().upper()
            if specialization_code is not None
            else ""
        )

        if specialization_code_normalized == "EMA-A":
            return "4"
        if self._contains_phrases(
            normalized, HEAD_OF_PHRASES
        ) or function_code_normalized.endswith("Z"):
            return "3"
        if self._match_stems(tokens, TEAMLEAD_STEMS) or self._contains_phrases(
            normalized, TEAMLEAD_PHRASES
        ):
            return "2"
        if self._match_stems(tokens, MANAGER_STEMS):
            return "1"
        return "1"

    def _process(self, row):
        bad = ["none", "nan", "-", ""]
        spec_col = "Код специализации"
        subfunction_col = "Код подфункции"
        if str(row[spec_col]).strip().lower() not in bad:
            return row[spec_col]
        return row[subfunction_col]

    def _resolve_column(self, df: pd.DataFrame, *candidates: str) -> str:
        for candidate in candidates:
            if candidate in df.columns:
                return candidate
        raise KeyError(f"Missing expected column. Tried: {candidates}")

    def _transformer_path(self) -> Path:
        return self.artifacts_dir / TRANSFORMER_FILENAME

    def _fit_transformer(self, df: pd.DataFrame) -> PowerTransformer:
        salary_col = self._resolve_column(df, BASE_SALARY_COL, "BP")
        salary = pd.to_numeric(df[salary_col], errors="coerce")
        valid_salary = salary.dropna()
        if valid_salary.empty:
            raise ValueError(
                "Cannot fit the Box-Cox transformer: salary column is empty."
            )
        if (valid_salary <= 0).any():
            raise ValueError(
                "Box-Cox transformer requires strictly positive salary values."
            )

        transformer = PowerTransformer(method="box-cox", standardize=False)
        transformer.fit(valid_salary.to_numpy(dtype=float).reshape(-1, 1))
        return transformer

    def _load_or_fit_transformer(
        self, df: pd.DataFrame, train: bool
    ) -> PowerTransformer:
        transformer_path = self._transformer_path()
        if transformer_path.exists():
            return joblib.load(transformer_path)
        if not train:
            raise FileNotFoundError(
                f"Transformer artifact not found: {transformer_path}"
            )

        transformer = self._fit_transformer(df)
        transformer_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(transformer, transformer_path)
        return transformer

    def prepare_features(self, df: pd.DataFrame, train: bool = False):
        df = df.copy()
        if SOURCE_ROW_ID_COL not in df.columns and not train:
            df[SOURCE_ROW_ID_COL] = np.arange(len(df))

        title_col = self._resolve_column(df, JOB_TITLE_COL, "job_title")
        df["code"] = df.progress_apply(lambda row: self._process(row), axis=1)
        df["seniority"] = df[title_col].progress_apply(
            lambda value: self._categorize_title(value)
        )

        categorical_features = [
            "subfunction_cleaned",
            "code_cleaned",
            "n_level_cleaned",
            "industry_cleaned",
            "headcount_cat_cleaned",
            "revenue_cat_cleaned",
            "seniority",
        ]
        numerical_features = ["Scaled_EmpBP_Portion_C", "Scaled_CR_SP_R", "subfunc_num"]

        print("Calculating features...")
        df = calculate_f_new(df)

        print(df.shape[0])
        if not train:
            source_row_ids = df[SOURCE_ROW_ID_COL].astype(int).to_numpy()
        df[categorical_features] = (
            df[categorical_features].fillna("missing").astype(str)
        )

        df_cat = df[categorical_features]
        df_num = df[numerical_features].astype(float)
        X = pd.concat([df_cat, df_num], axis=1)
        if not train:
            X.index = source_row_ids

        cat_feature_indices = [X.columns.get_loc(col) for col in categorical_features]
        y = (
            df["grade"]
            if "grade" in df.columns
            else pd.Series(index=df.index, dtype="float64")
        )
        if not train:
            y.index = source_row_ids
        return X, cat_feature_indices, y


class LeadPredictor:
    def __init__(
        self,
        iterations: int = 15000,
        path_to_model=None,
        artifacts_dir=None,
        task_type: str = "GPU",
    ):
        self.iterations = iterations
        self.artifacts_dir = (
            Path(artifacts_dir) if artifacts_dir else BASE_DIR / "grade_model_weights"
        )
        self.task_type = task_type

        if path_to_model:
            loaded = CatBoostRegressor()
            loaded.load_model(str(path_to_model))
            self.model = loaded
        else:
            self.model = CatBoostRegressor(
                loss_function="RMSE",
                eval_metric="RMSE",
                depth=9,
                iterations=self.iterations,
                learning_rate=0.03,
                min_data_in_leaf=5,
                random_seed=42,
                task_type=self.task_type,
                od_type="Iter",
                od_wait=50,
                verbose=True,
            )

    def train(
        self, df, output_dir=None, test_size: float = 0.1, random_state: int = 42
    ):
        output_dir = Path(output_dir) if output_dir else self.artifacts_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir = output_dir

        data_preparer = Dataset(df, artifacts_dir=output_dir)

        print("Preparing features...")
        X, cat_idx, y = data_preparer.prepare_features(df.copy(), train=True)
        self.cat_features = cat_idx

        print("Train-test split...")
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=random_state,
        )

        self.model.fit(
            X_train,
            y_train,
            eval_set=(X_test, y_test),
            cat_features=self.cat_features,
            verbose=True,
            use_best_model=True,
        )

        model_path = output_dir / MODEL_FILENAME
        self.model.save_model(str(model_path))
        print(f"Model saved in {model_path}")

        validation_pred = np.floor(self.model.predict(X_test) + 0.5).astype(int)
        y_test_int = y_test.astype(int).to_numpy()
        validation_rmse = float(np.sqrt(np.mean((y_test_int - validation_pred) ** 2)))
        validation_f1_macro = _compute_macro_f1(y_test_int, validation_pred)
        best_score = self.model.get_best_score()

        return {
            "model_path": str(model_path.resolve()),
            "transformer_path": str((output_dir / TRANSFORMER_FILENAME).resolve()),
            "train_rows": int(X_train.shape[0]),
            "validation_rows": int(X_test.shape[0]),
            "best_iteration": int(self.model.get_best_iteration()),
            "validation_rmse": validation_rmse,
            "validation_f1_macro": validation_f1_macro,
            "best_score": best_score,
        }

    def _predict_with_skips(self, X):
        if X.empty:
            return pd.Series(index=X.index, dtype="float64")

        try:
            return pd.Series(self.model.predict(X), index=X.index)
        except Exception as exc:
            raise RuntimeError(f"Batch prediction failed: {exc}") from exc

    def _attach_predictions(self, df, preds):
        out = df.copy()
        out[PREDICTION_COL] = pd.NA

        valid_preds = preds.dropna()
        if not valid_preds.empty:
            row_positions = valid_preds.index.to_numpy(dtype=int)
            valid_positions = (row_positions >= 0) & (row_positions < len(out))
            skipped_positions = len(row_positions) - int(valid_positions.sum())
            if skipped_positions:
                warnings.warn(
                    f"Skipping {skipped_positions} predictions with invalid source row ids.",
                    RuntimeWarning,
                )

            row_positions = row_positions[valid_positions]
            values = np.floor(
                valid_preds.to_numpy(dtype=float)[valid_positions] + 0.5
            ).astype(int)
            out.iloc[row_positions, out.columns.get_loc(PREDICTION_COL)] = values
        return out

    def predict(self, df, return_features: bool = False):
        prepare = Dataset(df, artifacts_dir=self.artifacts_dir)
        print("Preparing features...")
        X, _, _ = prepare.prepare_features(df, train=False)
        print("Predicting...")
        preds = self._predict_with_skips(X)
        print("Mapping predictions...")
        df_preds = self._attach_predictions(df, preds)
        if return_features:
            return df_preds, X
        return df_preds
