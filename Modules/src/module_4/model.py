import numpy as np
import pandas as pd
import re
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from tqdm import tqdm
import pandas as pd
import re
import pandas as pd
import numpy as np
import json
import os
import sys
import warnings
from typing import Iterable
from datetime import datetime
import joblib
from sklearn.preprocessing import PowerTransformer
from grade_model.utils import calculate_f_new

tqdm.pandas(desc="Processing")
warnings.filterwarnings("ignore", category=FutureWarning, message=".*DataFrameGroupBy.apply operated on the grouping columns.*")
# warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*invalid value encountered in log*")

TEAMLEAD_STEMS: set[str] = { "управляющ", "teamlead", "lead", "начальник", "руковод", "директор",'президент'}
TEAMLEAD_PHRASES: tuple[str, ...] = ("team lead",)
SENIOR_STEMS: set[str] = {"ведущ", "глав", "senior",'прораб'}
MIDDLE_STEMS: set[str] = {"старш", "mid", "middle"}
JUNIOR_STEMS: set[str] = {"junior", "jr", "младш", "начинающ"}

 
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
    
    def _process(self,row):
        bad = ['none','nan','-','']
        if not (str(row['Код специализации']).strip().lower() in bad):
            return row['Код специализации']
        else:
            return row['Код подфункции']
            

    def prepare_features(
            self,
            df: pd.DataFrame,
            train
        ):
            if train:
                pt = PowerTransformer(method='box-cox', standardize=True)
                df['BP_boxcox'] = pt.fit_transform(df[['Базовый оклад (BP)']])
                joblib.dump(pt, 'src/module_4/grade_model/bp_boxcox_transformer.pkl')
                df['seniority'] = df['job_title'].progress_apply(lambda x: self._categorize_title(x))
            else:
                pt = joblib.load('src/module_4/grade_model/bp_boxcox_transformer.pkl')
                df['BP_boxcox'] = pt.fit_transform(df[['Базовый оклад (BP)']])
                df['code'] = df.progress_apply(lambda x: self._process(x), axis=1)
                df['seniority'] = df['Название должности'].progress_apply(lambda x: self._categorize_title(x))
            
            df['Базовый оклад (BP)'] = df['BP_boxcox']
            print(f"Lambda Box-Cox: {pt.lambdas_[0]}")
            
            categorical_features = ['code', 'industry_cleaned', 'region_cleaned', 'headcount_cat_cleaned', 'revenue_cat_cleaned', 'seniority']
            numerical_features = ['Scaled_BP','Scaled_BP_Region','Scaled_BP_Code','Scaled_BP_rcs','STH_C','STH_SP','STH_C_R',
            'Scaled_EmpBP_Portion_C','Scaled_CR_SP_R','subfunc_num']
            
            df = df.drop_duplicates()
            print("Calculating features...")
            df = calculate_f_new(df)
            df[categorical_features] = df[categorical_features].fillna("missing").astype(str)

            df_cat = df[categorical_features]
            df_num = df[numerical_features].astype(float)
            X = pd.concat([df_cat, df_num], axis=1)

            cat_feature_indices = [X.columns.get_loc(col) for col in categorical_features]

            return X, cat_feature_indices, df['grade']


class GradePredictor:
    def __init__(self, iterations: int = 5000, path_to_model=None):
        self.iterations = iterations

        if path_to_model:
            loaded = CatBoostRegressor()
            loaded.load_model(path_to_model)
            self.model = loaded
        else:
            self.model = CatBoostRegressor(
                loss_function='Quantile:alpha=0.6',
                eval_metric='Quantile:alpha=0.6',
                depth=10,
                iterations=self.iterations,
                learning_rate=0.03,
                random_seed=42,
                task_type="GPU",
                od_type="Iter",
                od_wait=50,
                verbose=True
            )

    def train(self,df):
        df = df.copy()
        data_preparer = Dataset(df)

        print("Preparing features...")
        X, cat_idx, y = data_preparer.prepare_features(df,train=True)
        self.cat_features = cat_idx

        print("Train-test split...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=0.1,
            random_state=42
        )

        self.X_test = X_test
        self.y_test = y_test
        self.data = df

        self.model.fit(
            X_train, y_train,
            eval_set=(X_test, y_test),
            cat_features=self.cat_features,
            verbose=True,
            use_best_model =True
        )
        
        base_name = "models/model"
        ext = ".cbm"

        timestamp = datetime.now().strftime("%d_%m_%H_%M")
        filename = f"{base_name}_{timestamp}{ext}"

        self.model.save_model(filename)
        print(f"Model saved in {filename}")
        return X_test
    
    def _attach_predictions(self, df, idx_test, preds):
        out = df.loc[idx_test].copy()
        out['Грейд / Уровень обзора'] = np.floor(preds+0.5).astype(int)

        with open("src/module_4/grade_model/codes.json", "r", encoding="utf-8") as f:
            codes = json.load(f)
        
        # print(df['Грейд / Уровень обзора'])
        data = Dataset()
        out['code'] = out.progress_apply(lambda x: data._process(row=x), axis=1)
        out['description'] = out['code'].apply(lambda x: codes[x]['description'])
        out = out.drop(columns=['code'])
        return out
    

    def predict(self,df):
        prepare = Dataset(df)
        print("Preparing features...")
        X, _, _ = prepare.prepare_features(df, train=False)
        print("Predicting...")
        preds = self.model.predict(X)
        print("Mapping predictions...")
        df_preds = self._attach_predictions(df, df.index, preds)
        return df_preds
