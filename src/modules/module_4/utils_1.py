import re
from collections import defaultdict
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

E5_MODEL_NAME = "intfloat/multilingual-e5-base"
E5_EMBEDDING_DIM = 768
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")

JOB_TITLE_CATALOGS = {
    "ru": {
        "filename": "module_4/models/model_1/Salary_data_rus_2026.xlsx",
        "sheet_name": "Каталог функций",
        "code_columns": ("Код функции", "Код подфункции", "Код специализации"),
        "examples_column": "Примеры должностей",
    },
    "en": {
        "filename": "module_4/models/model_1/Salary data_eng_2026.xlsx",
        "sheet_name": "Functions",
        "code_columns": ("Function Code", "Subfunction Code", "Specialization Code"),
        "examples_column": "Job titles examples",
    },
}


def is_empty_value(x):
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
    text = re.sub(r"[^a-zа-яё0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_title_text(text):
    if is_empty_value(text):
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def _normalize_code(value):
    if is_empty_value(value):
        return ""
    return str(value).strip().upper()


def _detect_title_language(title) -> str:
    return "ru" if CYRILLIC_RE.search(normalize_title_text(title)) else "en"


def _split_job_title_examples(value) -> list[str]:
    if is_empty_value(value):
        return []
    examples = []
    seen = set()
    for part in str(value).split(","):
        example = normalize_title_text(part)
        if example and example not in seen:
            examples.append(example)
            seen.add(example)
    return examples


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.rename(
        columns={
            "Название должности": "job_title",
            "Код подфункции": "subfunction",
            "Код специализации": "spec",
            "Код функции": "function",
            "Грейд / Уровень обзора": "grade",
        },
        inplace=True,
    )
    return df


def calculate_f_new(df: pd.DataFrame) -> pd.DataFrame:
    df = _rename_columns(df)

    if "job_title" not in df.columns:
        df["job_title"] = ""
    if "subfunction" not in df.columns:
        df["subfunction"] = ""
    if "spec" not in df.columns:
        df["spec"] = ""
    if "code" not in df.columns:
        df["code"] = np.where(
            df["spec"].map(is_empty_value),
            df["subfunction"],
            df["spec"],
        )

    df["job_title_cleaned"] = df["job_title"].apply(normalize_title_text)
    df["subfunction_cleaned"] = df["subfunction"].apply(_normalize_code)
    df["code_cleaned"] = df["code"].apply(_normalize_code)
    return df


class E5TextEmbedder:
    def __init__(
        self,
        artifacts_dir: str | Path | None = None,
        model_name: str = E5_MODEL_NAME,
        embedding_dim: int = E5_EMBEDDING_DIM,
        batch_size: int = 32,
        max_length: int = 128,
    ):
        self.model_name = model_name
        self.embedding_dim = embedding_dim
        self.batch_size = batch_size
        self.max_length = max_length
        self.artifacts_dir = Path(artifacts_dir) if artifacts_dir else None
        self.embedding_cache_path = (
            self.artifacts_dir / "job_title_embedding_cache.pkl"
            if self.artifacts_dir
            else None
        )

        self._tokenizer = None
        self._model = None
        self._device = None
        self._cache = self._load_embedding_cache()
        self._zero_vector = np.zeros(self.embedding_dim, dtype=np.float32)

    def _load_embedding_cache(self) -> dict[str, np.ndarray]:
        if not self.embedding_cache_path or not self.embedding_cache_path.exists():
            return {"": np.zeros(self.embedding_dim, dtype=np.float32)}

        try:
            cache = joblib.load(self.embedding_cache_path)
        except Exception:
            return {"": np.zeros(self.embedding_dim, dtype=np.float32)}
        if "" not in cache:
            cache[""] = np.zeros(self.embedding_dim, dtype=np.float32)
        return cache

    def _save_embedding_cache(self) -> None:
        if not self.embedding_cache_path:
            return
        self.embedding_cache_path.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(self._cache, self.embedding_cache_path)

    def _load_model(self) -> None:
        if self._model is not None and self._tokenizer is not None:
            return

        try:
            import torch
            from transformers import AutoModel, AutoTokenizer
        except ImportError as exc:
            raise ImportError(
                "E5 embeddings require installed 'torch' and 'transformers' packages."
            ) from exc

        cache_dir = self.artifacts_dir / "hf_cache" if self.artifacts_dir else None
        load_kwargs = {}
        if cache_dir and cache_dir.exists():
            load_kwargs = {"cache_dir": str(cache_dir), "local_files_only": True}

        self._tokenizer = AutoTokenizer.from_pretrained(self.model_name, **load_kwargs)
        self._model = AutoModel.from_pretrained(self.model_name, **load_kwargs)
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._model.to(self._device)
        self._model.eval()

    def _encode_batch(self, texts: list[str]) -> np.ndarray:
        self._load_model()

        import torch

        batch_texts = [f"query: {text}" for text in texts]
        encoded = self._tokenizer(
            batch_texts,
            max_length=self.max_length,
            padding=True,
            truncation=True,
            return_tensors="pt",
        )
        encoded = {key: value.to(self._device) for key, value in encoded.items()}

        with torch.no_grad():
            outputs = self._model(**encoded)
            attention_mask = encoded["attention_mask"]
            masked = outputs.last_hidden_state.masked_fill(
                ~attention_mask[..., None].bool(), 0.0
            )
            embeddings = masked.sum(dim=1) / attention_mask.sum(dim=1)[..., None]
            embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
        return embeddings.cpu().numpy().astype(np.float32)

    def encode(self, texts) -> np.ndarray:
        normalized_texts = [normalize_title_text(text) for text in texts]
        missing_texts = []
        seen = set()

        for text in normalized_texts:
            if text not in self._cache and text not in seen:
                missing_texts.append(text)
                seen.add(text)

        non_empty_missing = [text for text in missing_texts if text]
        if non_empty_missing:
            for start in range(0, len(non_empty_missing), self.batch_size):
                batch = non_empty_missing[start : start + self.batch_size]
                batch_embeddings = self._encode_batch(batch)
                for text, embedding in zip(batch, batch_embeddings, strict=True):
                    self._cache[text] = embedding
            self._save_embedding_cache()

        return np.vstack(
            [self._cache.get(text, self._zero_vector) for text in normalized_texts]
        ).astype(np.float32)


class JobTitleExampleNormalizer:
    def __init__(
        self,
        text_embedder: E5TextEmbedder,
        catalog_dir: str | Path | None = None,
    ):
        self.text_embedder = text_embedder
        self.catalog_dir = Path(catalog_dir) if catalog_dir else PROJECT_ROOT
        self._examples_by_language_and_code: dict[str, dict[str, tuple[str, ...]]] = {}
        self._candidate_embedding_cache: dict[tuple[str, str], np.ndarray] = {}

    def _load_examples(self, language: str) -> dict[str, tuple[str, ...]]:
        if language in self._examples_by_language_and_code:
            return self._examples_by_language_and_code[language]

        config = JOB_TITLE_CATALOGS[language]
        path = self.catalog_dir / config["filename"]
        if not path.exists():
            raise FileNotFoundError(f"Job title catalog is missing: {path}")

        catalog = pd.read_excel(
            path,
            sheet_name=config["sheet_name"],
            header=4,
            dtype=str,
        )

        examples_by_code: dict[str, list[str]] = defaultdict(list)
        seen_by_code: dict[str, set[str]] = defaultdict(set)
        for _, row in catalog.iterrows():
            examples = _split_job_title_examples(row.get(config["examples_column"]))
            if not examples:
                continue

            for code_column in config["code_columns"]:
                code = _normalize_code(row.get(code_column))
                if not code:
                    continue
                for example in examples:
                    if example not in seen_by_code[code]:
                        examples_by_code[code].append(example)
                        seen_by_code[code].add(example)

        result = {code: tuple(examples) for code, examples in examples_by_code.items()}
        self._examples_by_language_and_code[language] = result
        return result

    def _candidate_embeddings(
        self, language: str, code: str, examples: tuple[str, ...]
    ) -> np.ndarray:
        cache_key = (language, code)
        if cache_key not in self._candidate_embedding_cache:
            self._candidate_embedding_cache[cache_key] = self.text_embedder.encode(
                list(examples)
            )
        return self._candidate_embedding_cache[cache_key]

    def normalize(self, titles, codes) -> list[str]:
        normalized_titles = [normalize_title_text(title) for title in titles]
        normalized_codes = [_normalize_code(code) for code in codes]
        result = normalized_titles.copy()

        grouped_rows: dict[tuple[str, str], list[int]] = defaultdict(list)
        for idx, (title, code) in enumerate(
            zip(normalized_titles, normalized_codes, strict=True)
        ):
            if not title or not code:
                continue
            grouped_rows[(_detect_title_language(title), code)].append(idx)

        for (language, code), row_indices in grouped_rows.items():
            examples = self._load_examples(language).get(code)
            if not examples:
                continue

            row_titles = [normalized_titles[idx] for idx in row_indices]
            title_embeddings = self.text_embedder.encode(row_titles)
            example_embeddings = self._candidate_embeddings(language, code, examples)
            similarities = title_embeddings @ example_embeddings.T
            nearest_indices = similarities.argmax(axis=1)

            for row_idx, example_idx in zip(row_indices, nearest_indices, strict=True):
                result[row_idx] = examples[int(example_idx)]

        return result
