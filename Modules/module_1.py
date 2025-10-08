import nbimporter
from src.module_1.main import main 
import sys 

# sys.path.append('src/module_1')
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

main(input_folder='src/module_1/companies/rus/', output_folder='src/module_1/output')
