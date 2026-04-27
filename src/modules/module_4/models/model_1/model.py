import re
import warnings
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent

from models.model_1.utils import (  # noqa: E402
    E5TextEmbedder,
    JobTitleExampleNormalizer,
    calculate_f_new,
)

tqdm.pandas(desc="Processing")

SOURCE_ROW_ID_COL = "__source_row_id__"
PREDICTION_COL = "predicted_grade"
JOB_TITLE_COL = "Название должности"
MODEL_FILENAME = "model.cbm"
SUB_MODEL_FEATURE_COLUMNS = (
    "subfunction_cleaned",
    "code_cleaned",
    "job_title_cleaned",
    "seniority",
)

SENIOR_STEMS: set[str] = {"глав", "senior"}
MIDDLE_PLUS_PLUS_STEMS: set[str] = {"ведущ"}
MIDDLE_PLUS_STEMS: set[str] = {"старш", "прораб"}
JUNIOR_STEMS: set[str] = {"junior", "jr", "младш", "начинающ", "l2"}
INTERN_STEMS: tuple[str, ...] = (
    "стажер",
    "стажёр",
    "intern",
    "интерн",
    "l1",
    "ассистент",
)


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
        self.text_embedder = E5TextEmbedder(artifacts_dir=self.artifacts_dir)
        self.job_title_normalizer = JobTitleExampleNormalizer(self.text_embedder)

    def _match_stems(
        self, tokens: Iterable[str], stems: set[str] | tuple[str, ...]
    ) -> bool:
        return any(tok.startswith(stem) for tok in tokens for stem in stems)

    def _categorize_title(
        self, title: str | None, function_code: str | None = None
    ) -> str:
        # del function_code

        if not title or not isinstance(title, str):
            return "middle"

        normalized = re.sub(r"[^\w\s]", " ", title.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        tokens = normalized.split()

        if (
            self._match_stems(tokens, ("глав", "старш", "senior", "ведущ"))
            and function_code == "IT"
        ):
            return "senior"
        if self._match_stems(tokens, INTERN_STEMS):
            return "стажер"
        if self._match_stems(tokens, JUNIOR_STEMS):
            return "младший"
        if self._match_stems(tokens, ("глав",)):
            return "главный"
        if self._match_stems(tokens, ("senior",)):
            return "senior"
        if self._match_stems(tokens, MIDDLE_PLUS_PLUS_STEMS):
            return "ведущий"
        if self._match_stems(tokens, ("старш",)):
            return "старший"
        if self._match_stems(tokens, ("прораб",)):
            return "прораб"

    def _process(self, row):
        bad = ["none", "nan", "-", ""]
        spec_col = "Код специализации"
        subfunction_col = "Код подфункции"
        if str(row.get(spec_col, "")).strip().lower() not in bad:
            return row.get(spec_col, "")
        return row.get(subfunction_col, "")

    def _resolve_column(self, df: pd.DataFrame, *candidates: str) -> str:
        for candidate in candidates:
            if candidate in df.columns:
                return candidate
        raise KeyError(f"Missing expected column. Tried: {candidates}")

    def prepare_features(self, df: pd.DataFrame, train: bool = False):
        del train

        df = df.copy()
        if SOURCE_ROW_ID_COL not in df.columns:
            df[SOURCE_ROW_ID_COL] = np.arange(len(df))

        title_col = self._resolve_column(df, JOB_TITLE_COL, "job_title")
        function_col = "Код функции" if "Код функции" in df.columns else "function"
        original_job_titles = df[title_col].copy()

        if "code" not in df.columns:
            df["code"] = df.progress_apply(lambda row: self._process(row), axis=1)
        df["seniority"] = df.progress_apply(
            lambda row: self._categorize_title(
                row.get(title_col), row.get(function_col)
            ),
            axis=1,
        )

        print("Calculating features...")
        df = calculate_f_new(df)
        source_row_ids = df[SOURCE_ROW_ID_COL].astype(int).to_numpy()

        print("Normalizing job titles with catalog examples...")
        df["job_title_cleaned"] = self.job_title_normalizer.normalize(
            df["job_title_cleaned"].tolist(),
            df["code_cleaned"].tolist(),
        )

        categorical_features = list(SUB_MODEL_FEATURE_COLUMNS)
        df[categorical_features] = (
            df[categorical_features]
            .replace("", "missing")
            .fillna("missing")
            .astype(str)
        )

        X = df[categorical_features].copy()
        X.index = source_row_ids
        original_job_titles.index = source_row_ids

        features = pd.concat(
            [
                X.reset_index(drop=True),
                original_job_titles.reset_index(drop=True).rename("original_job_title"),
            ],
            axis=1,
        )
        features.index = source_row_ids

        cat_feature_indices = [X.columns.get_loc(col) for col in categorical_features]
        y = df["grade"]
        y.index = source_row_ids
        return X, cat_feature_indices, y, features


class SubPredictor:
    def __init__(
        self,
        iterations: int = 17000,
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
            self._validate_loaded_model_schema(path_to_model)
        else:
            self.model = CatBoostRegressor(
                loss_function="RMSE",
                eval_metric="RMSE",
                depth=6,
                iterations=self.iterations,
                learning_rate=0.05,
                min_data_in_leaf=20,
                random_seed=42,
                task_type=self.task_type,
                od_type="Iter",
                od_wait=70,
                verbose=True,
                use_best_model=True,
            )

    def _validate_loaded_model_schema(self, path_to_model) -> None:
        loaded_feature_names = list(getattr(self.model, "feature_names_", []) or [])
        if not loaded_feature_names:
            return

        expected_feature_names = list(SUB_MODEL_FEATURE_COLUMNS)
        if loaded_feature_names != expected_feature_names:
            raise RuntimeError(
                "Loaded SubPredictor model has incompatible feature schema. "
                f"Expected {expected_feature_names}, got {loaded_feature_names}. "
                f"Path: {path_to_model}"
            )

    def train(
        self, df, output_dir=None, test_size: float = 0.1, random_state: int = 42
    ):
        output_dir = Path(output_dir) if output_dir else self.artifacts_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir = output_dir

        data_preparer = Dataset(df, artifacts_dir=output_dir)

        print("Preparing features...")
        X, cat_idx, y, _ = data_preparer.prepare_features(df.copy(), train=True)
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
            "embedding_cache_path": str(
                (output_dir / "job_title_embedding_cache.pkl").resolve()
            ),
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
        X, _, _, features = prepare.prepare_features(df, train=False)
        print("Predicting...")
        preds = self._predict_with_skips(X)
        print("Mapping predictions...")
        df_preds = self._attach_predictions(df, preds)
        if return_features:
            return df_preds, features
        return df_preds
