import pandas as pd
import re
import spacy

# def preprocess_data(df):
#     df = df.reset_index(drop=True)
#     df = df[:-11]
#     df.drop(columns=['Регион/область (заполняется автоматически)', 'Грейд / Уровень обзора', 'Код подфункции', 'Код специализации', 'Название подфункции (заполняется автоматически)', 'Название специализации (заполняется автоматически)', 'Общее количество сотрудников по состоянию на 1 мая 2025 года', 'Выручка за 2024 год, руб.', 'Тип компании'], inplace=True)
#     df.rename(columns={'Подразделение 1 уровня': 'p1', 'Подразделение 2 уровня': 'p2','Подразделение 3 уровня': 'p3','Подразделение 4 уровня': 'p4','Подразделение 5 уровня': 'p5','Подразделение 6 уровня': 'p6', 'Код функции': 'function', 'Сектор': 'industry', 'Название должности':'job_title', 'Название функции (заполняется автоматически)': 'function_name'}, inplace=True)
#     df.drop(columns=['function_name'], inplace=True)

def clean_add_info(text: str) -> str:
    text = str(text)
    if text == 'nan':
        text = ''
    text = remove_company_names(text)
    text = remove_named_entities(text)
    text = text.lower()
    # Убираем "г", "г.", "город", "города"
    text = re.sub(r'\bг\.?\b|\bгород\b|\bгорода\b', '', text, flags=re.IGNORECASE)
    # Убираем числа, но сохраняем "1С" (латиница/кириллица)
    text = re.sub(r'(?<!1[сc])\d+', '', text, flags=re.IGNORECASE)
    # Убираем слова вида "бригада 09" (и подобные конструкции из stems)
    stems = ['блок','региональн','групп','регион','област','отделение','отдел',
             'участок','основн','общий','центр','департамент','служб','район',
             'управление', 'направление', 'подразделение', 'филиал','дирекция',
             'бригад', 'комплекс', 'структурное', 'дивизион', 'цех']
    parts = [r'р-?н(?:а)?'] + sorted([re.escape(s) + r'\w*' for s in stems], key=len, reverse=True)
    regex = re.compile(r'\b(?:' + '|'.join(parts) + r')\s*\d*\b', re.UNICODE)
    text = regex.sub('', text)
    pattern = r'\b(?:по|в|и)\b'
    text = re.sub(pattern,'', text)
    # Удаляем лишние символы (кроме &, -)
    text = re.sub(r'[^a-zа-яё0-9\s&_-]', '', text)
    # Убираем дефисы в начале и конце
    text = re.sub(r'^-+|-+$', '', text)
    # Нормализуем пробелы
    text = re.sub(r'\s+', ' ', text).strip()
    # text = text.strip()
    return text

# Удаляем ООО "Тбанк" и тд
def remove_company_names(text: str) -> str:
    # Приводим к нижнему регистру для унификации, если нужно
    text = text.strip()

    # Удаляем выражения вида ООО "Название", АО 'Название', ПАО «Название» и т.д.
    text = re.sub(r'\b(ооо|zao|ao|oao|пао|зао|ooo|aoo|нко|акционерное общество)\s*["«“„\'`]+[^"»“”\'`]+["»””\'`]+', '', text, flags=re.IGNORECASE)

    # Также очищаем от двойных пробелов и лишних пробелов по краям
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# Job title
def sanitize_text(text):
    text = str(text)
    text = text.strip()
    text = text.lower()
    # Ensure only one space between words
    text = re.sub(r'\s+', ' ', text)
    # Remove hyphens at the start or end of the text
    text = re.sub(r'^-+|-+$', '', text)
    # Keep only allowed characters, including & and -
    text = re.sub(r'[^a-zа-яё0-9\s&-]', '', text)
    
    # Remove any sequence of six digits at the start, with optional whitespace handling
    text = re.sub(r'^\s*\d{6}', '', text)
    # Remove '- band [number]' and '- band bta'
    text = re.sub(r'- band \d+', '', text)
    text = re.sub(r'- band bta', '', text)
    # Remove 'cat [number]'
    text = re.sub(r'cat \d+', '', text)
    # Remove any number at the end of the text
    text = re.sub(r'\s+\d+$', '', text)
    # Remove '[number] разряда', '[number]-й разряд', '[number] разряд', '[number]-разряд' anywhere in the text
    text = re.sub(r'\b\d+(-го|-й|-)?\s?разряд(а)?\b', '', text)
    # Remove standalone Roman numerals (i to ix) followed by 'категории' anywhere in the text
    text = re.sub(r'\b(i|ii|iii|iv|v|vi|vii|viii|ix)\sкатегории\b', '', text)
    # Remove 's', 'm', 'l', 'xl' at the end if they are standalone
    text = re.sub(r'\b(s|m|l|xl)\b$', '', text)
    # Remove '[number] категории' and '[number]-й категории' anywhere in the text
    text = re.sub(r'\b\d+(-й)?\sкатегории\b', '', text)
    # Remove '[number] линия', '[number]-й линии', '[number] линии' anywhere in the text
    text = re.sub(r'\b\d+(-й)?\sлинии?\b', '', text)
    text = re.sub(r'\b\d+(-й)?\sлиния?\b', '', text)
    # Remove '[number]-го уровня' anywhere in the text
    text = re.sub(r'\b\d+-го уровня\b', '', text)
    # Remove '[number] р' anywhere in the text (updated to handle attached 'р')
    text = re.sub(r'\b\d+\s*р\b', '', text)
    text = re.sub(r'\b\d+р\b', '', text)  # New line to handle attached 'р'
    # Remove ordinal indicators at the start of the text
    text = re.sub(r'^\b(\d+-й|\d+nd|\d+rd|\d+st)\b\s*', '', text)
    # Remove '[number]-й' and '[number]-ый' at the start of the text
    text = re.sub(r'^\d+-(й|ый)\s*', '', text)
    # Remove '[number] кат' and '[number]кат' anywhere in the text
    text = re.sub(r'\b\d+\s?кат\b', '', text)
    # Remove '[number] смены' anywhere in the text
    text = re.sub(r'\b\d+\sсмены\b', '', text)
    # Remove '[number] квалификационный разряд' anywhere in the text
    text = re.sub(r'\b\d+\sквалификационный\sразряд\b', '', text)
    # Remove '[number] раз' anywhere in the text
    text = re.sub(r'\b\d+\sраз\b', '', text)
    
    # Remove numbers followed by variations of 'ур' or 'уровня'
    text = re.sub(r'\b\d+\s*(ур|уровня)\b', '', text)
    # Remove numbers followed by variations of 'категория' with possible typos
    text = re.sub(r'\b(\d+|первая|вторая|третья|четвертая|пятая|шестая|седьмая|восьмая|девятая|десятая)(-й|-я|-ой)?\s*(категори(?:я|и|ии|и|ю)?|кат)\b', '', text)
    # Remove 'без категории'
    text = re.sub(r'\bбез\s*категори(?:и|ю)\b', '', text)
    # Remove 'рейд-[number]' patterns
    text = re.sub(r'\bрейд-\d+\b', '', text)
    # Remove numbers followed by incomplete 'разряд' words with typos and spaces
    text = re.sub(r'\b\d+(-го|-й|-)?\s*(?:разр(?:яд|я|я|а|)|ра\s?зр(?:яд|я|я|а)?|разря)\b', '', text)
    # Remove '[number] блок' or 'блоку'
    text = re.sub(r'\b\d+\s*блок(у)?\b', '', text)
    # Remove '[number] грейд'
    text = re.sub(r'\b\d+\s*грейд\b', '', text)
    # Remove 'зоны [letter]'
    text = re.sub(r'\bзоны\s+[a-zа-яё]\b', '', text)
    # Remove 'категории [letter]'
    text = re.sub(r'\bкатегори(?:я|и|ии|и|ю)?\s+[a-zа-яё]\b', '', text)
    # Remove 'п г п[number]р' patterns
    text = re.sub(r'\bп\s?г\s?п\d+\s*р\b', '', text)
    # Remove 'очереди' following numbers or number ranges
    text = re.sub(r'\b\d+(-\d+)?\s*очереди\b', '', text)
    # Remove 'пу [number]'
    text = re.sub(r'\bпу\s*\d+\b', '', text)
    # Remove 'пресс [number]-го р'
    text = re.sub(r'\bпресс\s*\d+-го\s*р\b', '', text)
    # Remove 'кдм-[number]'
    text = re.sub(r'\bкдм-\d+\b', '', text)
    # Remove 'dwdmsdh'
    text = re.sub(r'\bdwdmsdh\b', '', text)
    # Remove any remaining numbers at the end of the text
    text = re.sub(r'\s+\d+$', '', text)

    # Remove 'что соответствует' and everything after it
    text = re.sub(r'что\s+соответствует.*$', '', text)
    
    # Handle cases where number is directly attached to 'разряда' with no space
    text = re.sub(r'(\S+)(\d+)\s+разряда', r'\1', text)

    # Remove standalone Roman numeral at the end of text
    text = re.sub(r'\s+\b(i|ii|iii|iv|v|vi|vii|viii|ix|x)\b$', '', text)

    # Remove Roman numeral followed by 'группа'
    text = re.sub(r'\s+\b(i|ii|iii|iv|v|vi|vii|viii|ix|x)\s+группа\b', '', text) # ???

    # Handle cases like 'машинист заверточных машин 4р2ак' -> 'машинист заверточных машин'
    text = re.sub(r'\s+\d+р\d+[а-яa-z]*\b', '', text)

    text = re.sub(r'\s+занят\s+на.*$', '', text)
    text = re.sub(r'(\S+)?разряда(\S+)?', r'\1 \2', text)
    text = re.sub(r'(\S+)(\d+)\s+категории', r'\1', text)
    text = re.sub(r'\s+\S+\s+категории', '', text)
    text = re.sub(r'(?<=\bдивизиона)(?:\s+\S+)+', '', text)
    text = re.sub(r'\s+региона(?:\s+\S+)*', '', text)
    text = re.sub(r'(\s+регионов)(?:\s+\S+)+', r'\1', text)

    # Remove instances of 'подъема', 'группы', 'гр', 'класса', etc. anywhere in the text
    # text = re.sub(r'\b\d+\sподъема\b', '', text)
    # text = re.sub(r'\b\d+\s(группы|гр|класса|кл)\b', '', text)
    # text = re.sub(r'\b\d+-я\sгруппа\b', '', text)
    # text = re.sub(r'\b(i|ii|iii|iv|v|vi|vii|viii|ix)\s(группы|класса)\b', '', text)
    
    # Keep only allowed characters, including & and -
    text = re.sub(r'[^a-zа-яё0-9\s&-]', '', text)
    # Remove hyphens at the start or end of the text
    text = re.sub(r'^-+|-+$', '', text)
    # Final strip to clean any remaining whitespace at start or end
    text = text.strip()
    return text


nlp = spacy.load("ru_core_news_lg")
def remove_named_entities(row):
    doc = nlp(row)
    result = ''
    list_ents = [str(i) for i in doc.ents]
    for token in doc:
        if str(token) not in list_ents:
            result += str(token)
            result += ' '
    return result


# Function to remove seniority terms from a job title string
def remove_seniority_from_string(text):
    """
    Removes seniority terms from a job title string
    
    Parameters:
    text (str): The job title text
    
    Returns:
    str: Job title with seniority terms removed
    """
    text = str(text)
    # List of seniority terms in Russian and English
    seniority_terms = [
        "младший", "мл",
        "ведущий", "вед",
        "главный", "гл",
        "старший", "ст",
        "senior", "sr",
        "junior", "jr",
        "middle", "md",
        '005','1','10','12','15','1к','1-категории','2','20','25857','2к','2-категории','3','3р1ак','3р1к','3р2к','4','4-го','4р3к','5','5р3к','5р4к','6','8','junior','senior','sr','vk','авито','ведущий','главный','ио','младший','мтс', 'ст','старший','теле2','уфо','х5',
    ]
    
    # Create a regex pattern to match any of the terms
    pattern = r'\b(?:' + '|'.join(seniority_terms) + r')\b'
    # Remove seniority terms
    text = re.sub(pattern, '', text)
    # Ensure only one space between words and trim
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text
