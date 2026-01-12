from src.module_1.main import module_1
from src.module_2.main import module_2
from src.module_3.main import module_3
from src.module_4.main import module_4
from src.module_5.main import module_5
from yaml import load, SafeLoader
import warnings

warnings.filterwarnings("ignore", category=UserWarning)


if __name__ == "__main__":
    modules = [module_1, module_2, module_3, module_4, module_5]

    params_for_each_module = [
        # module_1
        ['save_db_only_without_errors', 'drop_empty_month_salary', 'single_db'],
        # module_2
        ['folder_past_year', 'after_fix'],
        # module_3
        ['save_to_parquet'],
        # module_4
        ['folder_past_year', 'after_fix'],
        # module_5
        ['save_db_only_without_errors', 'drop_empty_month_salary', 'single_db'],
    ]

    with open('config.yml', 'r') as f:
        config = load(f, Loader=SafeLoader)

    input_folder = config['input_folder']
    output_folder = config['output_folder']
    module_num = config['module'] - 1

    param_names = params_for_each_module[module_num]
    params = {}
    for param in param_names:
        params[param] = config[param]
    modules[module_num](input_folder=input_folder, output_folder=output_folder, params=params)

