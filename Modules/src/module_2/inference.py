import os
import sys
import pandas as pd
from tqdm import tqdm


import os
import re
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from transformers import XLMRobertaTokenizer, XLMRobertaModel
import joblib
from IPython.display import display, HTML
import warnings
from sklearn.exceptions import InconsistentVersionWarning
import random
import time
import spacy

# parent_dir = os.path.dirname(os.getcwd())
# sys.path.insert(0, parent_dir)
# sys.path.append(os.path.abspath(os.path.dirname(__file__)))

warnings.simplefilter("ignore", category=UserWarning, lineno=329, append=False)
warnings.filterwarnings('ignore', message='The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*',
                       category=FutureWarning)
pd.set_option('future.no_silent_downcasting', True)

from LP import *
from function_model.clean_utils import clean_add_info, remove_seniority_from_string, sanitize_text


warnings.filterwarnings("ignore", category=InconsistentVersionWarning)
warnings.filterwarnings("ignore", message=".*Torch was not compiled with flash attention.*", category=UserWarning)


# Check if GPU is available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"Using device: {device}")
pd.set_option('display.max_columns', None)


# Define directories
results_dir = 'results_test'
# os.makedirs(results_dir, exist_ok=True)
# files_dir = 'data/dozagruzka/'
MODEL_DIR = "src/module_2/function_model/final_models"  # Directory where model files are stored


def predict_codes(df, test=False):
    # Define categorical features for each model level
    categorical_features_1 = ['industry']
    categorical_features_2 = ['industry', 'function']
    categorical_features_3 = ['industry', 'function', 'subfunction']
    
    try:
        # Function model
        model_function, function_encoders = load_model_with_encoders(
            'Function', 
            categorical_features_1
        )
        
        # Subfunction model
        model_subfunction, subfunction_encoders = load_model_with_encoders(
            'Subfunction', 
            categorical_features_2
        )
        
        # Specialization model
        model_specialization, specialization_encoders = load_model_with_encoders(
            'Spec', 
            categorical_features_3,
            model_class=SpecializationModel
        )
        
        print("\nAll models and encoders loaded successfully!")

        models = [model_function, model_subfunction, model_specialization]
        encoders = [function_encoders, subfunction_encoders, specialization_encoders]
        # Create combined industry mapping
        industry_mapping = {}

        
        
        
        companies = df[company_name].unique()
        print(f"Компании с пропусками кодов: {companies}")
        df_final = pd.DataFrame()
        for company in companies:
            df_comp = df.loc[df[company_name]==company]
            # print("----")
            # print(df_comp['Сектор'])
            industry = list(df_comp['Сектор'])[0]
            if industry in function_encoders['industry'].classes_:
                matched_industry = industry
            else:
                # Try case-insensitive match
                industry_lower = industry.lower()
                for ind in function_encoders['industry'].classes_:
                    if ind.lower() == industry_lower:
                        matched_industry = ind
                        break
                        
                # If still not found, try partial match
                if not matched_industry:
                    for ind in function_encoders['industry'].classes_:
                        if industry_lower in ind.lower() or ind.lower() in industry_lower:
                            matched_industry = ind
                            break
            
            if not matched_industry:
                print(f"Warning: Industry '{industry}' not found in encoders")
                print(f"Available industries: {function_encoders['industry'].classes_}")
                return
            # 2. Process all files in the directory
            df_res = process_files_and_predict(df_comp, models, encoders, matched_industry, test)
            df_final = pd.concat([df_final, df_res])
        return df_final
        # print(f"Industry for this file: {list(set(industry))}")
        # matched_industry = list(set(industry))[0]


        # Use industry encoder from Function model as the base
        # for industry in function_encoders['industry'].classes_:
        #     # Create multiple entries with different formats for the same industry
        #     cleaned = sanitize_text(industry)
        #     industry_mapping[cleaned] = industry
        #     # Add the original version too
        #     industry_mapping[industry] = industry
        #     # Add a version with capitalization but no special chars
        #     simplified = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s]', '', industry)
        #     industry_mapping[simplified] = industry
        #     industry_mapping[simplified.lower()] = industry
        #     # Add version with special chars converted to spaces
        #     spacified = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9]', ' ', industry).strip()
        #     industry_mapping[spacified] = industry
        #     industry_mapping[spacified.lower()] = industry

    except Exception as e:
        print(f"Error in model loading process: {e}")
        raise



# Define model classes
class ClassificationModel(nn.Module):
    """Base classification model for job titles using XLM-RoBERTa and categorical features"""
    def __init__(self, num_cat_features, num_classes, categorical_features, roberta_model_name='xlm-roberta-base'):
        super(ClassificationModel, self).__init__()
        self.roberta = XLMRobertaModel.from_pretrained(roberta_model_name)
        self.cat_embedding = nn.Embedding(num_cat_features, 50)
        self.fc1 = nn.Linear(self.roberta.config.hidden_size + 50 * len(categorical_features), 256)
        self.fc2 = nn.Linear(256, num_classes)
        self.relu = nn.ReLU()
        self.categorical_features = categorical_features

    def forward(self, input_ids, attention_mask, cat_features):
        roberta_output = self.roberta(input_ids, attention_mask=attention_mask)
        roberta_output = roberta_output.pooler_output
        cat_features = self.cat_embedding(cat_features).view(cat_features.size(0), -1)
        x = torch.cat((roberta_output, cat_features), dim=1)
        x = self.fc1(x)
        x = self.relu(x)
        x = self.fc2(x)
        return x

class SpecializationModel(ClassificationModel):
    """Enhanced classification model for the specialization level with larger embeddings and dropout"""
    def __init__(self, num_cat_features, num_classes, categorical_features, roberta_model_name='xlm-roberta-base'):
        super(SpecializationModel, self).__init__(num_cat_features, num_classes, categorical_features, roberta_model_name)
        
        # Increased embedding dimension for categorical features
        embedding_dim = 64
        self.cat_embedding = nn.Embedding(num_cat_features, embedding_dim)
        
        # Increased hidden layer size
        hidden_size = 384
        self.fc1 = nn.Linear(self.roberta.config.hidden_size + embedding_dim * len(categorical_features), hidden_size)
        self.dropout = nn.Dropout(0.2)  # Added dropout for regularization
        self.fc2 = nn.Linear(hidden_size, num_classes)

    def forward(self, input_ids, attention_mask, cat_features):
        roberta_output = self.roberta(input_ids, attention_mask=attention_mask)
        roberta_output = roberta_output.pooler_output
        
        cat_features = self.cat_embedding(cat_features).view(cat_features.size(0), -1)
        
        x = torch.cat((roberta_output, cat_features), dim=1)
        x = self.fc1(x)
        x = self.relu(x)
        x = self.dropout(x)
        x = self.fc2(x)
        return x


# Enhanced function to load model with its specific encoders
def load_model_with_encoders(model_name, categorical_features, model_class=ClassificationModel):
    """
    Load a model and its associated encoders dynamically
    
    Parameters:
    model_name (str): Base name of the model (e.g., 'Function', 'Subfunction', 'Spec')
    categorical_features (list): List of categorical feature names
    model_class (nn.Module): The model class to instantiate
    
    Returns:
    tuple: (model, encoders_dict) - The loaded model and a dictionary of its encoders
    """
    # print(f"\nLoading {model_name} model and its encoders...")
    model_path = os.path.join(MODEL_DIR, f"{model_name}_final.pth")
    encoders_dict = {}
    
    try:
        # First load state dict to get dimensions
        state_dict = torch.load(model_path, map_location=device)
        num_cat_features = state_dict['cat_embedding.weight'].shape[0]
        embedding_dim = state_dict['cat_embedding.weight'].shape[1]
        
        # Load all associated encoders for this model
        for feature in categorical_features:
            # Special case for target (output) encoder
            if feature == 'target':
                encoder_path = os.path.join(MODEL_DIR, f"{model_name}_le_target.pkl")
            else:
                encoder_path = os.path.join(MODEL_DIR, f"{model_name}_le_{feature}.pkl")
            
            # Try to load model-specific encoder, fall back to generic if needed
            if os.path.exists(encoder_path):
                encoders_dict[feature] = joblib.load(encoder_path)
                # print(f"  - Loaded {feature} encoder for {model_name} model")
            else:
                # Look for generic encoder
                generic_path = os.path.join(MODEL_DIR, f"Function_le_{feature}.pkl")
                if os.path.exists(generic_path):
                    encoders_dict[feature] = joblib.load(generic_path)
                    # print(f"  - Used generic {feature} encoder for {model_name} model")
                else:
                    raise FileNotFoundError(f"Could not find encoder for {feature}")
        
        # Load output encoder (always model-specific)
        target_path = os.path.join(MODEL_DIR, f"{model_name}_le_target.pkl")
        encoders_dict['target'] = joblib.load(target_path)
        # print(f"  - Loaded target encoder for {model_name} model")
        
        # Get number of output classes
        num_classes = len(encoders_dict['target'].classes_)
        
        # Create and load model with correct dimensions
        # print(f"  - Creating {model_name} model with {num_cat_features} categorical features and {num_classes} output classes")
        model = model_class(num_cat_features, num_classes, categorical_features).to(device)
        model.load_state_dict(state_dict)
        model.eval()
        
        return model, encoders_dict
        
    except Exception as e:
        print(f"Error loading {model_name} model: {e}")
        raise


# Enhanced prediction functions for each level with their specific encoders and confidence scores
def enhanced_predict_function(model, tokenizer, text, industry, function_encoders):
    """Predict function category for a job title with confidence score"""
    inputs = tokenizer.encode_plus(text, add_special_tokens=True, max_length=128, padding='max_length', 
                                  return_attention_mask=True, return_tensors='pt', truncation=True)
    input_ids = inputs['input_ids'].to(device)
    attention_mask = inputs['attention_mask'].to(device)
    
    # Encode industry using function model's specific encoder
    encoded_industry = function_encoders['industry'].transform([industry])[0]
    industry_tensor = torch.tensor([encoded_industry], dtype=torch.long).to(device)
    
    with torch.no_grad():
        outputs = model(input_ids=input_ids, attention_mask=attention_mask, cat_features=industry_tensor)
    
    # Apply softmax to get probabilities
    probabilities = torch.nn.functional.softmax(outputs, dim=1)
    
    # Get predicted class and its probability
    pred_idx = outputs.argmax(dim=1).item()
    confidence = probabilities[0, pred_idx].item()
    
    return pred_idx, function_encoders['target'].inverse_transform([pred_idx])[0], confidence

def enhanced_predict_subfunction(model, tokenizer, text, industry, function, subfunction_encoders):
    """Predict subfunction category for a job title with confidence score"""
    inputs = tokenizer.encode_plus(text, add_special_tokens=True, max_length=128, padding='max_length', 
                                  return_attention_mask=True, return_tensors='pt', truncation=True)
    input_ids = inputs['input_ids'].to(device)
    attention_mask = inputs['attention_mask'].to(device)
    
    # Encode industry and function using subfunction model's specific encoders
    encoded_industry = subfunction_encoders['industry'].transform([industry])[0]
    encoded_function = subfunction_encoders['function'].transform([function])[0]
    
    features_tensor = torch.tensor([[encoded_industry, encoded_function]], dtype=torch.long).to(device)
    
    with torch.no_grad():
        outputs = model(input_ids=input_ids, attention_mask=attention_mask, cat_features=features_tensor)
    
    # Apply softmax to get probabilities
    probabilities = torch.nn.functional.softmax(outputs, dim=1)
    
    # Get predicted class and its probability
    pred_idx = outputs.argmax(dim=1).item()
    confidence = probabilities[0, pred_idx].item()
    
    return pred_idx, subfunction_encoders['target'].inverse_transform([pred_idx])[0], confidence

def enhanced_predict_specialization(model, tokenizer, text, industry, function, subfunction, specialization_encoders):
    """Predict specialization category for a job title with confidence score"""
    inputs = tokenizer.encode_plus(text, add_special_tokens=True, max_length=128, padding='max_length', 
                                  return_attention_mask=True, return_tensors='pt', truncation=True)
    input_ids = inputs['input_ids'].to(device)
    attention_mask = inputs['attention_mask'].to(device)
    
    # Encode industry, function and subfunction using specialization model's specific encoders
    encoded_industry = specialization_encoders['industry'].transform([industry])[0]
    encoded_function = specialization_encoders['function'].transform([function])[0]
    encoded_subfunction = specialization_encoders['subfunction'].transform([subfunction])[0]
    
    features_tensor = torch.tensor([[encoded_industry, encoded_function, encoded_subfunction]], dtype=torch.long).to(device)
    
    with torch.no_grad():
        outputs = model(input_ids=input_ids, attention_mask=attention_mask, cat_features=features_tensor)
    
    # Apply softmax to get probabilities
    probabilities = torch.nn.functional.softmax(outputs, dim=1)
    
    # Get predicted class and its probability
    pred_idx = outputs.argmax(dim=1).item()
    confidence = probabilities[0, pred_idx].item()
    
    return pred_idx, specialization_encoders['target'].inverse_transform([pred_idx])[0], confidence


def select_p_features(df):
    p_cols = [
        dep_level_1, dep_level_2, dep_level_3,
        dep_level_4, dep_level_5, dep_level_6
    ]

    clean = clean_add_info  # локальная ссылка для скорости

    combined = (
        df[p_cols]
        .astype(str)
        .replace(['nan', 'None', 'NaN'], '')
        .agg(' _ '.join, axis=1)
    )
    unique_texts = combined.drop_duplicates()

    mapping = {}
    for text in unique_texts:
        cleaned = clean(text)
        parts = [x.strip() for x in cleaned.split('_') if x.strip()]
        if len(parts) > 1:
            mapping[text] = parts[-2] + ' ' + parts[-1]
        elif len(parts) == 1:
            mapping[text] = parts[-1]
        else:
            mapping[text] = ''

    # Маппим результаты обратно на весь df
    results = combined.map(mapping)
    return results

def predict(df, matched_industry, models, encoders):

    # print("Loading tokenizer...")
    tokenizer = XLMRobertaTokenizer.from_pretrained('xlm-roberta-base')
    # print("Tokenizer loaded successfully.")

    model_function, model_subfunction, model_specialization = models
    function_encoders, subfunction_encoders, specialization_encoders = encoders

    unique_job_titles = df['text_input'].drop_duplicates().tolist()
    # print(f"Found {len(unique_job_titles)} unique job titles to process")

    # Dictionary to store predictions for unique job titles
    job_predictions = {}

    # Process each unique job title
    for job_title in tqdm(unique_job_titles, desc="Predicting unique job titles"):
        # try:
        # print("ok")
        # Clean job title
        # job_title_clean = sanitize_text(job_title)
        job_title_clean = remove_seniority_from_string(job_title)

        # print("OK!!!!!!!!!!!!!!!!!!!!")
        # print(job_title_clean)
        
        # Predict function with confidence
        _, function_name, function_confidence = enhanced_predict_function(
            model_function, tokenizer, job_title_clean, matched_industry, function_encoders
        )
        
        # Predict subfunction with confidence
        _, subfunction_name, subfunction_confidence = enhanced_predict_subfunction(
            model_subfunction, tokenizer, job_title_clean, matched_industry, 
            function_name, subfunction_encoders
        )
        
        # Predict specialization with confidence
        _, specialization_name, specialization_confidence = enhanced_predict_specialization(
            model_specialization, tokenizer, job_title_clean, matched_industry, 
            function_name, subfunction_name, specialization_encoders
        )
        
        # Store predictions and confidence scores for this job title
        job_predictions[job_title] = {
            function_code: function_name,
            'function_confidence': function_confidence,
            subfunction_code: subfunction_name,
            'subfunction_confidence': subfunction_confidence,
            specialization_code: specialization_name if specialization_name != '-' else None,
            'specialization_confidence': specialization_confidence if specialization_name != '-' else None
        }
            
        # print(job_predictions[job_title])
        # except Exception as e:
        #     print(f"Error processing '{job_title}': {str(e)}")
        #     job_predictions[job_title] = {
        #         'function_pred': f"ERROR: {str(e)}",
        #         'function_confidence': None
        #     }
    return job_predictions


#--------------------- Predict -------------------------
# Single function to process all files
def process_files_and_predict(df, models, encoders, matched_industry, test=False):
    """
    Function that loops through files in files_dir, 
    makes predictions, and saves results to results_dir.
    
    - Each file has one industry
    - We process only unique job titles to avoid redundant processing
    - Results are mapped back to all occurrences
    - Now includes confidence scores for each prediction
    """
    # Load df
    if test:
        df.drop(columns=[function_code], inplace=True)
    print(f"Loaded df with shape: {df.shape}")

    
    # Check required columns
    if 'Название должности' not in df.columns:
        print(f"Required column 'job_title' not found in df. ")
        return

    # df = df.reset_index(drop=True) 

    if test and ('add_info' in df.columns):
        # print("yes")
        df.drop(columns='add_info', inplace=True)
    if test and ('text_input' in df.columns):
        df.drop(columns='text_input',inplace=True)
    
    start = time.time()
    lst = select_p_features(df)
    finish = time.time()
    res_time = finish - start
    print(f"time: {res_time}")

    res_col = pd.Series(lst, name='add_info')
    df = pd.concat([df, res_col], axis=1)
    # print(df.columns)
    # print(df.index.is_unique)

    # # print(df)

    df = df.reset_index(drop=True) 

    df["text_input"] = (
        df['Название должности'].astype(str) +
        " [SEP] " + df["add_info"].astype(str)
    )

    # Get unique job titles
    job_predictions = predict(df, matched_industry, models, encoders)
    
    # print(job_predictions)
    # Apply predictions to the DataFrame
    print("Mapping predictions back to all occurrences...")
    
    # Initialize new columns
    df['function_pred'] = None
    df['function_confidence'] = None
    df['subfunction_pred'] = None
    df['subfunction_confidence'] = None
    df['specialization_pred'] = None
    df['specialization_confidence'] = None
    
    # Map the predictions back to all rows
    for job_title, predictions in tqdm(job_predictions.items()):
        # print(f"job title: {job_title}, preds: {predictions}")
        # Find all rows with this job title
        mask = df['text_input'] == job_title
        # Apply predictions and confidence scores
        for col, value in predictions.items():
            df.loc[mask, col] = value

    # print(df.loc[:20, 'function_pred'])

    # # Format confidence scores as percentages (optional)
    for col in ['function_confidence', 'subfunction_confidence', 'specialization_confidence']:
        mask = df[col].notna()
        if mask.any():
            df.loc[mask, col] = df.loc[mask, col].apply(lambda x: f"{x:.2%}")
    
    
    # print(df.loc[:20, 'function_pred'])
    # Save results
    # output_path = os.path.join(results_dir, f"{company_naming}_FunPreds_NoDeps_mymodel.xlsx")
    # company_naming +=1
    print(f"\nAll files processed!")
    return df
    