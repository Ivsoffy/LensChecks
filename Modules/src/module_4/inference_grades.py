
import os
import pandas as pd
import torch
from transformers import BertTokenizer, BertModel
from torch import nn
import joblib
from tqdm import tqdm
import numpy as np
import time
import Feature_buildnew as fb
from tqdm.notebook import tqdm
from IPython.display import display, HTML
import warnings

from LP import *

warnings.filterwarnings("ignore", message=".*Torch was not compiled with flash attention.*", category=UserWarning)


# Define the model class (needs to be identical to the training model definition)
class GradePredictionModel(nn.Module):
    def __init__(self, bert_model, num_classes_list, num_numerical_features, num_grade_classes, dropout_rate=0.3):
        super(GradePredictionModel, self).__init__()
        self.bert = bert_model
        self.dropout = nn.Dropout(dropout_rate)
        
        # Embeddings for categorical features
        self.embeddings = nn.ModuleList([
            nn.Embedding(num_classes, self.bert.config.hidden_size)
            for num_classes in num_classes_list
        ])
        
        # Calculate the combined dimension
        combined_dim = self.bert.config.hidden_size * (len(num_classes_list) + 1) + num_numerical_features
        
        # Deeper network with multiple layers
        self.fc1 = nn.Linear(combined_dim, 512)
        self.bn1 = nn.BatchNorm1d(512)
        self.fc2 = nn.Linear(512, 256)
        self.bn2 = nn.BatchNorm1d(256)
        self.fc3 = nn.Linear(256, num_grade_classes)
        
        # Activation functions
        self.relu = nn.ReLU()

    def forward(self, input_ids, attention_mask, categorical, numerical):
        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        pooled_output = outputs[1]
        pooled_output = self.dropout(pooled_output)
        
        # Embed categorical features
        embedded_features = [embedding(categorical[:, i]) for i, embedding in enumerate(self.embeddings)]
        
        # Combine all features
        combined_output = torch.cat([pooled_output] + embedded_features + [numerical], dim=1)
        
        # Forward through deep network
        x = self.fc1(combined_output)
        x = self.bn1(x)
        x = self.relu(x)
        x = self.dropout(x)
        
        x = self.fc2(x)
        x = self.bn2(x)
        x = self.relu(x)
        x = self.dropout(x)
        
        # Final layer
        grade = self.fc3(x)
        return grade

# Function to encode text using BERT tokenizer
def bert_encode(texts, tokenizer, max_len=128):
    input_ids = []
    attention_masks = []
    
    for text in texts:
        encoded = tokenizer.encode_plus(
            text,
            add_special_tokens=True,
            max_length=max_len,
            padding='max_length',
            return_attention_mask=True,
            return_tensors='pt',
            truncation=True
        )
        
        input_ids.append(encoded['input_ids'])
        attention_masks.append(encoded['attention_mask'])
    
    return torch.cat(input_ids, dim=0), torch.cat(attention_masks, dim=0)

# Inference function
def run_inference(df, model, tokenizer, encoders, le_grade, device, batch_size=128):
    """
    Run inference on the provided dataframe
    
    Args:
        df: Dataframe with features
        model: Trained model
        tokenizer: BERT tokenizer
        encoders: Dictionary of label encoders for categorical features
        le_grade: Label encoder for grade classes
        batch_size: Batch size for inference
    
    Returns:
        Dataframe with predictions and probabilities
    """
    model.eval()
    all_predictions = []
    all_probabilities = []
    
    # Define the features as in training
    categorical_features = ['function_cleaned', 'subfunction_cleaned', 'spec_cleaned', 'industry_cleaned', 'region_cleaned', 'headcount_cat_cleaned', 'revenue_cat_cleaned']
    numerical_features = ['Scaled_Logged_BP','Scaled_Logged_BP_Region', 'FtC','SUtC','SPtC','SUtF','SPtSU', 'functions_num',
                          'subfunctions_num', 'spec_num','Scaled_EmpBP_Portion_C','Scaled_EmpBP_Portion_F','Scaled_EmpBP_Portion_SU',
                          'Scaled_EmpBP_Portion_SP','Scaled_emp_in_job','Scaled_emp_in_job_r','Scaled_CR_C','Scaled_CR_F','Scaled_CR_SU',
                          'Scaled_CR_SP','Scaled_CR_C_R','Scaled_CR_F_R','Scaled_CR_SU_R','Scaled_CR_SP_R']
    
    # Encode categorical features
    X_categorical = []
    # print(df.loc[3, 'function'])
    for feature in categorical_features:
        encoder = encoders[feature]
        # Handle potential new categories not seen in training
        encoded_feature = []
        for value in df[feature]:
            try:
                encoded_value = encoder.transform([value])[0]
            except:
                # Assign a default value (0) for new categories not seen during training
                encoded_value = 0
                # print(f"Warning: New category '{value}' found in '{feature}' - using default encoding")
            encoded_feature.append(encoded_value)
        X_categorical.append(encoded_feature)
    
    X_categorical = np.array(X_categorical).T
    
    # Extract numerical features
    X_numerical = df[numerical_features].values
    
    # Tokenize job titles with progress bar
    print("Tokenizing job titles...")
    unique_job_titles = df['job_title_cleaned'].unique()
    print(f"Number of unique job titles: {len(unique_job_titles)}")
    
    unique_input_ids, unique_attention_masks = bert_encode(unique_job_titles, tokenizer)
    
    # Create a dictionary mapping job titles to their tokenized values
    tokenized_mapping = {job_title: (input_id, attention_mask) for job_title, input_id, attention_mask in 
                         zip(unique_job_titles, unique_input_ids, unique_attention_masks)}
    
    # Map tokenized values back to the original DataFrame
    X_input_ids = torch.stack([tokenized_mapping[job_title][0] for job_title in df['job_title_cleaned']])
    X_attention_masks = torch.stack([tokenized_mapping[job_title][1] for job_title in df['job_title_cleaned']])

    # for job_title in df['job_title']:
    #     print(job_title)
    #     print(X_input_ids)
    #     print(X_attention_masks)
        
    
    # Process in batches to avoid memory issues
    num_samples = len(df)
    num_batches = (num_samples + batch_size - 1) // batch_size
    
    print(f"Running inference on {num_samples} samples in {num_batches} batches...")
    progress_bar = tqdm(total=num_batches, desc="Processing batches")
    
    with torch.no_grad():
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, num_samples)
            
            # Get batch data
            batch_input_ids = X_input_ids[start_idx:end_idx].to(device)
            batch_attention_masks = X_attention_masks[start_idx:end_idx].to(device)
            batch_categorical = torch.tensor(X_categorical[start_idx:end_idx], dtype=torch.long).to(device)
            batch_numerical = torch.tensor(X_numerical[start_idx:end_idx], dtype=torch.float).to(device)
            
            # Run model
            grade_pred = model(batch_input_ids, batch_attention_masks, batch_categorical, batch_numerical)
            
            # Get predicted classes and probabilities
            probabilities = torch.softmax(grade_pred, dim=1)
            predictions = torch.argmax(grade_pred, dim=1)
            
            # Move to CPU and convert to numpy
            all_predictions.extend(predictions.cpu().numpy())
            all_probabilities.extend(probabilities.cpu().numpy())
            
            progress_bar.update(1)
    
    progress_bar.close()
    
    # Convert predictions back to original grade labels
    predicted_grades = le_grade.inverse_transform(all_predictions)
    
    # Add predictions to the dataframe
    df_result = df.copy()
    df_result[grade] = predicted_grades
    df_result['predicted_grade_encoded'] = all_predictions
    
    # Add top-3 predicted grades and their probabilities
    top_k = 3
    
    # Find indices of top-k probabilities for each prediction
    all_probs = np.array(all_probabilities)
    topk_indices = np.argsort(-all_probs, axis=1)[:, :top_k]
    
    # Extract corresponding grades and probabilities
    for i in range(top_k):
        # Skip if we don't have enough classes
        if i >= all_probs.shape[1]:
            continue
            
        grade_idx = topk_indices[:, i]
        probs = np.array([all_probs[j, idx] for j, idx in enumerate(grade_idx)])
        
        grade_labels = le_grade.inverse_transform(grade_idx)
        
        df_result[f'top{i+1}_grade'] = grade_labels
        df_result[f'top{i+1}_probability'] = probs
    
    # Calculate confidence (probability of the top prediction)
    df_result['prediction_confidence'] = [all_probs[i, pred] for i, pred in enumerate(all_predictions)]
    
    return df_result

# Main inference function
def predict_grades(df):
    orig_df = df.copy()
    # Check if CUDA is available and set the device to GPU if possible
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Define model directory and tag
    my_directory = 'src/module_4/grade_model'  # Same as training
    tag = "Grade"  # Same as training

    start_time = time.time()
    print("Starting grade model inference...")
    
    # Load model components
    print("Loading encoders and model files...")
    
    # Load the BERT tokenizer and model
    tokenizer = BertTokenizer.from_pretrained('bert-base-multilingual-cased')
    bert_model = BertModel.from_pretrained('bert-base-multilingual-cased').to(device)
    
    # Load label encoders
    categorical_features = ['function', 'subfunction', 'spec', 'industry', 'region', 'headcount_cat', 'revenue_cat']
    encoders = {}
    # print(os.getcwd())
    for feature in categorical_features:
        encoders[feature+'_cleaned'] = joblib.load(f'{my_directory}/{tag}_le_{feature}.pkl')
    
    le_grade = joblib.load(f'{my_directory}/{tag}_le_grade.pkl')
    
    # Get number of classes for each categorical feature
    num_classes_list = [len(encoder.classes_) for encoder in encoders.values()]
    num_grade_classes = len(le_grade.classes_)
    
    # Define the number of numerical features
    numerical_features = ['Scaled_Logged_BP','Scaled_Logged_BP_Region', 'FtC','SUtC','SPtC','SUtF','SPtSU', 'functions_num',
                          'subfunctions_num', 'spec_num','Scaled_EmpBP_Portion_C','Scaled_EmpBP_Portion_F','Scaled_EmpBP_Portion_SU',
                          'Scaled_EmpBP_Portion_SP','Scaled_emp_in_job','Scaled_emp_in_job_r','Scaled_CR_C','Scaled_CR_F','Scaled_CR_SU',
                          'Scaled_CR_SP','Scaled_CR_C_R','Scaled_CR_F_R','Scaled_CR_SU_R','Scaled_CR_SP_R']
    
    num_numerical_features = len(numerical_features)
    
    # Initialize the model
    print("Initializing model...")
    model = GradePredictionModel(bert_model, num_classes_list, num_numerical_features, num_grade_classes).to(device)
    
    # Load the best model weights
    best_model_path = f"{my_directory}/{tag}_best_f1_model.pth"
    print(f"Loading best model from {best_model_path}")
    model.load_state_dict(torch.load(best_model_path, map_location=device))
    model.eval()
    
    # Create output directory if it doesn't exist
    # output_dir = 'results'
    # os.makedirs(output_dir, exist_ok=True)
    # input_dir = 'files'
    
    # Load and process the data
    
    # ===============================================================
    # PLACEHOLDER: Load and transform your data here
    # Example:
    # df = pd.read_csv('your_data.csv')
    # 
    # Implement any necessary transformations to match the training data
    # The dataframe must contain the following columns:
    # - 'job_title': for BERT encoding
    # - categorical_features: ['function', 'subfunction', 'spec', 'industry', 'region', 'headcount_cat', 'revenue_cat']
    # - numerical_features: as defined in the variable numerical_features
    # ===============================================================
    
    start_time = time.time()
    companies = df[company_name].unique()
    print(f"Компании с пропусками кодов: {companies}")
    df_final = pd.DataFrame()
    for company in companies:
        df_comp = df.loc[df[company_name]==company]
        # print("----")
        # print(df_comp['Сектор'])
        industry = list(df_comp['Сектор'])[0]
        if industry in sector_map.values():
            matched_industry = industry
        
        if not matched_industry:
            print(f"Warning: Industry '{industry}' not found in encoders")
            print(f"Available industries: {sector_map.values()}")
            return
        # 2. Process all files in the directory
        df_res = process_files_and_predict(df, model, tokenizer, encoders, le_grade, device)
        df_final = pd.concat([df_final, df_res])

    # orig_df[grade].update(df_final['predicted_grade'])
    return df_final
    


def process_files_and_predict(df, model, tokenizer, encoders, le_grade, device):
    df = fb.calculate_f(df)
    # YOUR DATA TRANSFORMATION CODE HERE
    
    # ===============================================================
    # End of PLACEHOLDER
    # ===============================================================
    
    # Run inference
    print("Running inference...")
    # print(f"DEBUG: {df.loc[5,'job_title']}")
    df_with_predictions = run_inference(df, model, tokenizer, encoders, le_grade, device)
    # print(f"DEBUG: {df_with_predictions.loc[5,'job_title']}")
    df_with_predictions = df_with_predictions[['company', 'Подразделение 1 уровня','Подразделение 2 уровня','Подразделение 3 уровня',
                                                'Подразделение 4 уровня','Подразделение 5 уровня','Подразделение 6 уровня','job_title',
                                                'Код сотрудника','Код руководителя сотрудника','Руководитель / специалист',
                                                'Оценка эффективности работы сотрудника','Уровень подчинения по отношению к Первому лицу компании',
                                                'Экспат','Пол','Год рождения','Дата приема на работу','Сотрудники, проработавшие в компании меньше 1 года',
                                                'Название города','region','Внутренний грейд компании', grade, 'prediction_confidence','function','subfunction','spec',
                                                'Название функции (заполняется автоматически)','Название подфункции (заполняется автоматически)',
                                                'Название специализации (заполняется автоматически)','Размер ставки','Ежемесячный оклад',
                                                'Число окладов в году','Постоянные надбавки и доплаты (общая сумма за год)','Право на получение переменного вознаграждения',
                                                'Фактическая премия','Целевая премия (%)','Право на участие в Программе долгосрочного вознаграждения (LTIP)','Фактическая стоимость всех предоставленных типов LTI за 1 год',
                                                'Целевая стоимость всех предоставленных типов LTI в % от базового оклада за 1 год',
                                                'Тип программы 1','Фактическая стоимость вознаграждения 1 за 1 год','Целевая стоимость вознаграждения 1 как % от базового оклада за 1 год',
                                                'Частота выплат 1','Тип программы 2','Фактическая стоимость вознаграждения 2 за 1 год','Целевая стоимость вознаграждения 2 как % от базового оклада за 1 год',
                                                'Частота выплат 2','Тип программы 3','Фактическая стоимость вознаграждения 3 за 1 год','Целевая стоимость вознаграждения 3 как % от базового оклада за 1 год',
                                                'Частота выплат 3','Комментарии','Годовой оклад (AP)','BP','Краткосрочное фактическое переменное вознаграждение (VP)','Целевая Премия (TI)',
                                                'Фактическое совокупное вознаграждение (TC)','Целевое совокупное вознаграждение (TTC)','Фактическое долгосрочное вознаграждение (LTIP)',
                                                'Целевое долгосрочное вознаграждение (TLTIP)','Прямое совокупное вознаграждение (TDC)','Целевое прямое совокупное вознаграждение (TTDC)']]

    df_with_predictions = df_with_predictions.rename(columns={'company': company_name,
                                                                'job_title': job_title,
                                                                'region': region,
                                                                'function': function_code,
                                                                'subfunction': subfunction_code,
                                                                'spec': specialization_code,
                                                                'BP': base_pay})

    
    # Save results
    # output_path = f"{output_dir}/{company_name}_grade_inference.xlsx"

    # Записываем оба DataFrame-ы на отдельные листы в одном Excel файле.
    # with pd.ExcelWriter(output_path) as writer:
    #     df_info.to_excel(writer, sheet_name="Общая информация", startrow=2, startcol=1, header=False, index=False)
    #     df_with_predictions.to_excel(writer, sheet_name='Данные', index=False)
    
    # print(f"Results saved to {output_path}")
    
    # Print summary
    print("\nInference Summary:")
    print(f"Processed {len(df_with_predictions)} records")
    print(f"Number of unique predicted grades: {df_with_predictions[grade].nunique()}")
    # print(f"Grade distribution:")
    # print(df_with_predictions['predicted_grade'].value_counts().head(10))
    

    return df_with_predictions