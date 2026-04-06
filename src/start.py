import shutil
import warnings
from pathlib import Path

from modules.module_1.main import module_1
from modules.module_2.main import module_2
from modules.module_3.main import module_3
from modules.module_4.main import module_4
from modules.module_5.main import module_5
from yaml import SafeLoader, load

warnings.filterwarnings("ignore", category=UserWarning)


def load_config(config_path="config.yml"):
    with open(config_path, "r") as f:
        return load(f, Loader=SafeLoader)


def find_input_folder(input_folder, module, already_fixed):
    if input_folder and Path(input_folder).is_dir():
        return input_folder

    if module > 1:
        if (
            module != 2
            and module != 4
            or ((module == 2 or module == 4) and not already_fixed)
        ):
            input = "modules/module_" + str(module - 1) + "/output"
            if Path(input).is_dir():
                return input
            input = "modules/module_" + str(module) + "/input"
            if Path(input).is_dir():
                return input
        else:
            input = "modules/module_" + str(module) + "/output/require_check"
            if Path(input).is_dir():
                return input
    elif module == 1:
        input = "modules/module_" + str(module) + "/input"
        if Path(input).is_dir():
            return input

    return None


def find_output_folder(module, already_fixed):
    if (module != 2 and module != 4) or (
        (module == 2 or module == 4) and already_fixed
    ):
        output_folder = Path(f"modules/module_{module}/output")
    else:
        output_folder = Path(f"modules/module_{module}/output/require_check")
    output_folder.mkdir(parents=True, exist_ok=True)
    return output_folder


def cleaning_folders():
    src = Path("modules/module_5/output")
    dst = Path("modules/results")
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


def run_checks(config):
    modules = [module_1, module_2, module_3, module_4, module_5]

    params_for_each_module = [
        # module_1
        ["save_db_only_without_errors", "single_db"],
        # module_2
        ["folder_past_year", "after_fix"],
        # module_3
        [],
        # module_4
        ["folder_past_year", "after_fix"],
        # module_5
        ["save_db_only_without_errors", "single_db"],
    ]

    input_folder = config["input_folder"]
    module_num = config["module"] - 1

    input_folder = find_input_folder(
        input_folder, config["module"], config["after_fix"]
    )
    output_folder = find_output_folder(config["module"], config["after_fix"])
    if input_folder is None:
        print(f"Directory input_folder: {input_folder} not found!")
        if (config["module"] == 2) or (config["module"] == 4):
            print("Check parameter after_fix в config.yml!")
        return

    print("Directory to check: ", input_folder)
    print("Checked files will be saved in the directory: ", output_folder)

    param_names = params_for_each_module[module_num]
    params = {}
    for param in param_names:
        params[param] = config[param]
    module_result = modules[module_num](
        input_folder=input_folder, output_folder=output_folder, params=params
    )

    if (config["module"] == 5) and config["clean"]:
        cleaning_folders()

    if config["module"] == 1 or config["module"] == 5:
        return module_result


if __name__ == "__main__":
    config = load_config()
    run_checks(config)
