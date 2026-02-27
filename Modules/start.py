from src.module_1.main import module_1
from src.module_2.main import module_2
from src.module_3.main import module_3
from src.module_4.main import module_4
from src.module_5.main import module_5

from yaml import load, SafeLoader
from pathlib import Path
import warnings
import shutil

warnings.filterwarnings("ignore", category=UserWarning)


def find_input_folder(input_folder, module, already_fixed):
    if module > 1:
        if module != 2 and module != 4 or ((module == 2 or module == 4) and not already_fixed):
            input = 'src/module_'+str(module-1)+'/output'
            if Path(input).is_dir():
                return input
            input = 'src/module_'+str(module)+'/input'
            if Path(input).is_dir():
                return input
        else:
            input = 'src/module_'+str(module)+'/output/require_check'
            if Path(input).is_dir():
                return input
    elif module == 1:
        input = 'src/module_'+str(module)+'/input'
        print(input)
        if Path(input).is_dir():
            return input
    
    if input_folder and Path(input_folder).is_dir():
        return input_folder
    
    return None

def find_output_folder(module,already_fixed):
    if (module != 2 and module != 4) or ((module == 2 or module == 4) and already_fixed):
        output_folder = Path(f'src/module_{module}/output')
    else:
        output_folder = Path(f'src/module_{module}/output/require_check')
    output_folder.mkdir(parents=True, exist_ok=True)
    return output_folder

def cleaning_folders():
    src = Path('src/module_5/output')
    dst = Path('src/results')
    dst.mkdir(exist_ok=True)
    for p in src.iterdir():
        if p.is_file():
            shutil.copy2(p, dst / p.name)
    
    base = Path("src")
    archive = base / "archive"

    for i in range(1, 6):
        for name in ("input", "output"):
            src_dir = base / f"module_{i}" / name
            if src_dir.is_dir():
                dst_dir = archive / f"module_{i}" / name
                dst_dir.mkdir(parents=True, exist_ok=True)
                for item in src_dir.iterdir():
                    dst_item = dst_dir / item.name
                    if dst_item.exists():
                        if dst_item.is_dir():
                            shutil.rmtree(dst_item)
                        else:
                            dst_item.unlink()
                    shutil.move(str(item), str(dst_item))

def main():
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
    module_num = config['module'] - 1

    input_folder = find_input_folder(input_folder, config['module'], config['after_fix'])
    output_folder = find_output_folder(config['module'], config['after_fix'])
    if input_folder == None:
        print(f'Директория input_folder: {input_folder} с анкетами не найдена!')
        if (config['module'] == 2) or (config['module'] == 4):
            print(f"Проверьте параметр after_fix в config.yml!")
        return
    
    print("Проверяемая директория: ", input_folder)
    print("Проверенные файлы будут сохранены в директорию ", output_folder)

    param_names = params_for_each_module[module_num]
    params = {}
    for param in param_names:
        params[param] = config[param]
    modules[module_num](input_folder=input_folder, output_folder=output_folder, params=params)

    if (config['module'] == 5) and (config['clean'] == True):
        cleaning_folders()

if __name__ == "__main__":
    main()