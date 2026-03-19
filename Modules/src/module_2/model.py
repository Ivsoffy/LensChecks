import json
import re
from pathlib import Path

import evaluate
import numpy as np
import pandas as pd
import torch
from datasets import Dataset
from src.LP import (
    dep_level_1,
    dep_level_2,
    dep_level_3,
    dep_level_4,
    dep_level_5,
    dep_level_6,
    job_title,
)
from tqdm import tqdm
from transformers import (
    DataCollatorForSeq2Seq,
    EarlyStoppingCallback,
    Seq2SeqTrainer,
    Seq2SeqTrainingArguments,
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
        self.rouge = evaluate.load("rouge")

    def _prepare_output_dir(self, base_dir: str) -> str:
        base_path = Path(base_dir)
        if not base_path.exists():
            base_path.mkdir(parents=True, exist_ok=False)
            return str(base_path)

        idx = 1
        while True:
            candidate = base_path.with_name(f"{base_path.name}_{idx}")
            if not candidate.exists():
                candidate.mkdir(parents=True, exist_ok=False)
                return str(candidate)
            idx += 1

    def _preprocess_for_train(self, batch):
        inputs = ["summarize: " + x for x in batch["input_text"]]

        model_inputs = self.tokenizer(
            inputs, max_length=128, truncation=True, padding="max_length"
        )

        labels = self.tokenizer(
            text_target=batch["target_text"],
            max_length=64,
            truncation=True,
            padding="max_length",
        )

        model_inputs["labels"] = labels["input_ids"]
        return model_inputs

    def _load_codes(self):
        with open("function_model/codes_2026.json", "r", encoding="utf-8") as f:
            codes = json.load(f)

        print(f"Loaded {len(codes)} classification codes")
        return codes

    def _process_row(self, row, train=False):
        if train:
            deps = ["p1", "p2", "p3", "p4", "p5", "p6"]
            res = row["industry"]
            job = row["job_title"]
        else:
            deps = [
                dep_level_1,
                dep_level_2,
                dep_level_3,
                dep_level_4,
                dep_level_5,
                dep_level_6,
            ]
            res = row["Сектор"]
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

        df["input_text"] = df.apply(lambda x: self._process_row(x, train=True), axis=1)
        df["target_text"] = df.apply(lambda x: self._process_target(x), axis=1)

        df = df[["input_text", "target_text"]].dropna()
        df["input_text"] = df["input_text"].astype(str)
        df["target_text"] = df["target_text"].astype(str)

        return df

    def _norm_text(self, s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    def _compute_metrics(self, eval_pred):
        preds = eval_pred.predictions
        labels = eval_pred.label_ids

        if isinstance(preds, tuple):
            preds = preds[0]

        preds = np.where(preds < 0, self.tokenizer.pad_token_id, preds)
        labels = np.where(labels != -100, labels, self.tokenizer.pad_token_id)

        pred_str = self.tokenizer.batch_decode(preds, skip_special_tokens=True)
        label_str = self.tokenizer.batch_decode(labels, skip_special_tokens=True)

        pred_str = [self._norm_text(s) for s in pred_str]
        label_str = [self._norm_text(s) for s in label_str]

        rouge = self.rouge.compute(predictions=pred_str, references=label_str)

        return {
            "rougeL": float(rouge["rougeL"]),
            "rouge1": float(rouge["rouge1"]),
            "rouge2": float(rouge["rouge2"]),
        }

    def train(self, df):
        """
        Запускает обучение модлели.
        df: DataFrame = датафрейм, содержащий колонки industry, p1-p6, job_title, code.
        """
        df = df.copy()
        df = self._prepare_dataset(df)

        dataset = Dataset.from_pandas(df)
        splits = dataset.train_test_split(test_size=0.1, seed=42)
        train_ds = splits["train"]
        val_ds = splits["test"]

        train_tok = train_ds.map(
            self._preprocess_for_train,
            batched=True,
            remove_columns=train_ds.column_names,
        )
        val_tok = val_ds.map(
            self._preprocess_for_train, batched=True, remove_columns=val_ds.column_names
        )

        output_dir = self._prepare_output_dir("./ruT5_job_normalization")
        print("Output dir will be: ", output_dir)

        training_args = Seq2SeqTrainingArguments(
            output_dir=output_dir,
            eval_strategy="steps",
            save_strategy="steps",
            eval_steps=300,
            save_steps=300,
            logging_steps=100,
            save_total_limit=3,
            learning_rate=1e-4,
            lr_scheduler_type="linear",
            warmup_ratio=0.06,
            weight_decay=0.01,
            max_grad_norm=1.0,
            per_device_train_batch_size=16,
            per_device_eval_batch_size=16,
            gradient_accumulation_steps=8,
            num_train_epochs=10,
            load_best_model_at_end=True,
            metric_for_best_model="rougeL",
            greater_is_better=True,
            optim="adamw_torch",
            fp16=True,
            predict_with_generate=True,
            generation_max_length=164,
            generation_num_beams=3,
            label_smoothing_factor=0.1,
        )

        data_collator = DataCollatorForSeq2Seq(
            self.tokenizer, model=self.model, label_pad_token_id=-100
        )

        trainer = Seq2SeqTrainer(
            model=self.model,
            args=training_args,
            train_dataset=train_tok,
            eval_dataset=val_tok,
            tokenizer=self.tokenizer,
            data_collator=data_collator,
            compute_metrics=self._compute_metrics,
            callbacks=[EarlyStoppingCallback(early_stopping_patience=3)],
        )

        trainer.train()

        trainer.save_model(output_dir)
        self.tokenizer.save_pretrained(output_dir)

        print("Модель успешно обучена и сохранена!")

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
