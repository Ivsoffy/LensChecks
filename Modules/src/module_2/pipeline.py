from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F
import pandas as pd
import time
from tqdm import tqdm
from typing import Callable, TypeVar, Any, Tuple
import json
import os
import sys
import json
from LP import job_title

sys.path.append('src/module_2/model/')
from model import FunctionModel

ckpt = "src/module_2/model/ruT5_job_normalization_9"
device = "cuda" if torch.cuda.is_available() else "cpu"

 
class CodeModel:
    def __init__(self, name_encode="intfloat/multilingual-e5-base"):
        self.tokenizer = AutoTokenizer.from_pretrained(name_encode)
        self.model_encode = AutoModel.from_pretrained(name_encode).to(device)
        self.model_encode.eval()
        self.model_norm = FunctionModel(model=ckpt)

    def _embed(self, sentences, batch_size=64):
        embeddings = []
        for i in range(0, len(sentences), batch_size):
            batch_sents = sentences[i:i + batch_size]
            batch = self.tokenizer(
                batch_sents,
                padding=True,
                truncation=True,
                return_tensors="pt",
                max_length=128
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
        print(f"Similarity between s1 and s2: {cos_sim[0,1].item():.4f}")
        return cos_sim[0,1].item()

    def predict(self, df, test=False):
        df = df.copy()
        print("Preparing data...")
        # print('Normalization job_titles..')
        df = self.model_norm.predict(df)

        print('Loading codes..')

        with open("src/module_2/model/codes.json", "r", encoding="utf-8") as f:
            codes = json.load(f)

        # codes = self.model_norm._load_codes()
        code_ids = list(codes.keys())

        def _clean_text(val):
            if val is None:
                return ""
            s = str(val).strip()
            if s.lower() in {"", "nan", "none", "null", "na", "n/a", "nil", "undefined"}:
                return ""
            return s

        code_descs = [_clean_text(codes[code]['description']) for code in code_ids]
        code_desc_embeds = self._embed(code_descs)
        
        print('Creating embeddings..')
        # parts = df['predicted_desc'].fillna("").astype(str).str.partition(".")
        # df['desc'] = parts[0].str.strip()
        # df['exp'] = parts[2].str.strip()
        pred_desc_embeds = self._embed(df['predicted_desc'].tolist())
        pred_exp_embeds = self._embed(df[job_title].tolist())
        print("Calculate cosine simularity..")
        normalized_pred_desc = F.normalize(pred_desc_embeds, dim=1)
        normalized_pred_exp = F.normalize(pred_exp_embeds, dim=1)
        normalized_code_desc = F.normalize(code_desc_embeds, dim=1)
        cos_desc = normalized_pred_desc @ normalized_code_desc.T

        topk_inds = torch.topk(cos_desc, k=3, dim=1).indices
        topk_codes = [[code_ids[i] for i in row] for row in topk_inds.tolist()]
        example_embed_cache = {}
        best_codes = []
        best_sims = []
        best_examples = []

        for row_idx in range(df.shape[0]):
            candidates = topk_inds[row_idx].tolist()
            best_code = code_ids[candidates[0]]
            best_sim = cos_desc[row_idx, candidates[0]].item()
            best_example = ""
            best_example_sim = float("-inf")

            # best_func = codes[best_code].get('type')
            # if best_func == 'subfunction':
            #     print("debug")


            for cand_idx in candidates:
                code_id = code_ids[cand_idx]
                examples_raw = codes[code_id].get('examples')
                examples = [p.strip() for p in str(examples_raw).split(",") if p.strip()]
                if not examples:
                    continue

                if code_id not in example_embed_cache:
                    ex_embeds = self._embed(examples)
                    example_embed_cache[code_id] = F.normalize(ex_embeds, dim=1)

                ex_norm = example_embed_cache[code_id]
                sims = (normalized_pred_exp[row_idx].unsqueeze(0) @ ex_norm.T).squeeze(0)
                max_idx = sims.argmax().item()
                max_sim = sims[max_idx].item()
                if max_sim > best_example_sim:
                    best_example_sim = max_sim
                    best_example = examples[max_idx]
                    best_sim = max_sim
                    # best_code = code_id
                if max_sim > best_sim:
                    best_sim = max_sim
                    best_code = code_id

            best_codes.append(best_code)
            best_sims.append(best_sim)
            best_examples.append(best_example)

        df['top3_codes'] = topk_codes
        df['best_example'] = best_examples
        df['predicted_code'] = best_codes
        # store max cosine similarity per row
        df['predicted_code_similarity'] = best_sims
        print("Codes predicted!")
        if test==True:
            # df['check'] = df.apply(lambda x: 1 if x['predicted_code'] == x['code'] else 0, axis=1)
            df['description'] = df['predicted_code'].apply(lambda x: codes[x]['description'])
        return df

