import json
import os
import re
import sys

import torch
import torch.nn.functional as F
from transformers import AutoModel, AutoTokenizer

parent_dir = os.path.dirname(os.getcwd())
sys.path.insert(0, parent_dir)

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from .. import LP  # noqa: E402

sys.path.append("modules/module_2/function_model_weights/")
from model import FunctionModel  # noqa: E402

ckpt = "modules/module_2/function_model_weights/model_weights"
device = "cuda" if torch.cuda.is_available() else "cpu"

langs = {
    "H": [".net", "c#", "csharp"],
    "L": ["c", "c++"],
    "M": ["go", "golang", "го"],
    "N": ["delphi", "делфи"],
    "O": ["java", "джава"],
    "R": ["perl"],
    "S": ["php", "backend"],
    "T": ["python", "питон"],
    "U": ["ruby", "руби"],
    "V": ["scala", "скала"],
}

LANG_PATTERNS = {
    key: [re.compile(rf"(?<!\w){re.escape(lang.lower())}(?!\w)") for lang in values]
    for key, values in langs.items()
}


class CodeModel:
    def __init__(self, name_encode="intfloat/multilingual-e5-base"):
        self.tokenizer = AutoTokenizer.from_pretrained(name_encode)
        self.model_encode = AutoModel.from_pretrained(name_encode).to(device)
        self.model_encode.eval()
        self.model_norm = FunctionModel(model=ckpt)

        with open(
            "modules/module_2/function_model_weights/codes.json", "r", encoding="utf-8"
        ) as f:
            self.codes = json.load(f)
        self.code_ids = list(self.codes.keys())
        code_descs = [
            self._clean_text(self.codes[code]["description"]) for code in self.code_ids
        ]
        self.code_desc_embeds = self._embed(code_descs)
        self.normalized_code_desc = F.normalize(self.code_desc_embeds, dim=1)

    def _clean_text(self, val):
        if val is None:
            return ""
        s = str(val).strip()
        if s.lower() in {"", "nan", "none", "null", "na", "n/a", "nil", "undefined"}:
            return ""
        return s

    @staticmethod
    def _normalize_sentence(value):
        if value is None:
            return ""
        text = str(value).strip()
        if text.lower() in {
            "",
            "nan",
            "none",
            "null",
            "na",
            "n/a",
            "nil",
            "undefined",
            "<na>",
        }:
            return ""
        return text

    def _embed(self, sentences, batch_size=64):
        sentences = [self._normalize_sentence(sentence) for sentence in sentences]
        embeddings = []
        for i in range(0, len(sentences), batch_size):
            batch_sents = sentences[i : i + batch_size]
            batch = self.tokenizer(
                batch_sents,
                padding=True,
                truncation=True,
                return_tensors="pt",
                max_length=128,
            ).to(device)

            with torch.inference_mode():
                outputs = self.model_encode(**batch)
                cls_embeddings = outputs.last_hidden_state[:, 0]
            embeddings.append(cls_embeddings.cpu())
        return torch.cat(embeddings, dim=0)

    def _calc_cosine_simularity(self, sentences):
        print("Tokenization..")
        cls_embeddings = self._embed(sentences)
        print("Calculate cosine simularity..")

        emb_norm = F.normalize(cls_embeddings, p=2, dim=1)
        cos_sim = emb_norm @ emb_norm.T
        print(f"Similarity between s1 and s2: {cos_sim[0, 1].item():.4f}")
        return cos_sim[0, 1].item()

    def _detect_ita_lang_suffix(self, job_title):
        normalized_title = self._normalize_sentence(job_title).lower()
        if not normalized_title:
            return None
        for key, patterns in LANG_PATTERNS.items():
            if any(pattern.search(normalized_title) for pattern in patterns):
                return key
        return None

    def it_langs(self, df):
        ita_mask = df["predicted_code"] == "ITA"
        if ita_mask.any():
            ita_suffixes = df.loc[ita_mask, LP.job_title].apply(
                self._detect_ita_lang_suffix
            )
            matched_idx = ita_suffixes[ita_suffixes.notna()].index
            df.loc[matched_idx, "predicted_code"] = (
                df.loc[matched_idx, "predicted_code"]
                + "-"
                + ita_suffixes.loc[matched_idx]
            )
        return df

    def predict(self, df, test=False):
        df = df.copy()
        print("Preparing data...")
        df = self.model_norm.predict(df)

        print("Creating embeddings..")
        pred_desc_embeds = self._embed(df["predicted_desc"].tolist())
        print("Calculate cosine simularity..")
        normalized_pred_desc = F.normalize(pred_desc_embeds, dim=1)
        cos_desc = normalized_pred_desc @ self.normalized_code_desc.T

        best_idx = torch.argmax(cos_desc, dim=1)
        best_codes = [self.code_ids[i] for i in best_idx.tolist()]

        df["predicted_code"] = best_codes
        if LP.job_title in df.columns:
            df = self.it_langs(df)

        print("Codes predicted!")
        # if test:
        #     # df['check'] = df.apply(lambda x: 1 if x['predicted_code'] == x['code'] else 0, axis=1)
        #     df["description"] = df["predicted_code"].apply(
        #         lambda x: best_codes[x]["description"]
        #     )
        return df
