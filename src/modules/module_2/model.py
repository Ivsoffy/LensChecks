import json
import re

import pandas as pd
import torch
from modules.LP import (
    dep_level_1,
    dep_level_2,
    dep_level_3,
    dep_level_4,
    dep_level_5,
    dep_level_6,
    gi_sector,
    job_title,
)
from tqdm import tqdm
from transformers import (
    T5ForConditionalGeneration,
    T5TokenizerFast,
)

device = "cuda" if torch.cuda.is_available() else "cpu"
tqdm.pandas()
MISSING_STRINGS = {"", "nan", "none", "null", "na", "n/a", "nil", "undefined"}


class FunctionModel:
    def __init__(self, model: str = "sberbank-ai/ruT5-base"):
        self.tokenizer = T5TokenizerFast.from_pretrained(model)
        self.model = T5ForConditionalGeneration.from_pretrained(model).to(device)

    def _load_codes(self):
        with open("function_model/codes_2026.json", "r", encoding="utf-8") as f:
            codes = json.load(f)

        print(f"Loaded {len(codes)} classification codes")
        return codes

    def _process_row(self, row):
        deps = [
            dep_level_1,
            dep_level_2,
            dep_level_3,
            dep_level_4,
            dep_level_5,
            dep_level_6,
        ]
        res = row[gi_sector]
        job = row[job_title]
        for dep in deps:
            val = row.get(dep)
            if pd.notna(val) and str(val).strip():
                res += ". " + str(val).strip()
        res += ". " + str(job).strip() + "."
        return res

    def _process_target(self, row):
        codes = self.codes
        res = str(codes[row["code"]]["description"])
        return res

    def _prepare_dataset(self, df):
        self.codes = self._load_codes()

        valid_codes = set(self.codes.keys())
        print("Размер датасета: ", df.shape[0])
        df_not_in = df.loc[~df["code"].isin(valid_codes)]
        df = df.loc[df["code"].isin(valid_codes)]

        print("Размер датасета после удаления несуществующих кодов: ", df.shape[0])
        print(f"Несуществующие коды: {list(set(df_not_in['code']))}")

        df["input_text"] = df.apply(lambda x: self._process_row(x), axis=1)
        df["target_text"] = df.apply(lambda x: self._process_target(x), axis=1)

        df = df[["input_text", "target_text"]].dropna()
        df["input_text"] = df["input_text"].astype(str)
        df["target_text"] = df["target_text"].astype(str)

        return df

    def _norm_text(self, s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    def predict(self, df, batch_size=64, num_beams=4):
        self.model.eval()
        df["input_text"] = df.progress_apply(lambda x: self._process_row(x), axis=1)
        print("Predicting...")
        inputs = df["input_text"].tolist()
        preds = []
        for i in tqdm(range(0, len(inputs), batch_size)):
            batch = inputs[i : i + batch_size]
            batch = ["summarize: " + x for x in batch]
            encoded = self.tokenizer(
                batch,
                return_tensors="pt",
                max_length=128,
                truncation=True,
                padding=True,
            ).to(device)
            with torch.inference_mode():
                gen_ids = self.model.generate(
                    **encoded, max_length=64, num_beams=num_beams, early_stopping=True
                )
            preds.extend(self.tokenizer.batch_decode(gen_ids, skip_special_tokens=True))
        df["predicted_desc"] = preds
        return df
