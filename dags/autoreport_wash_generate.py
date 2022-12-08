import os
import yaml
from jinja2 import Environment, FileSystemLoader
from utils import PtbwaUtils

ptbwa_utils = PtbwaUtils()

file_dir = os.path.dirname(os.path.abspath(__file__))
_BASE_PATH = os.path.join(file_dir, "ymls")
ptbwa_utils.check_result("FILE_DIR", file_dir)
ptbwa_utils.check_result("BASE_PATH", _BASE_PATH)

env = Environment(loader=FileSystemLoader(file_dir))
template = env.get_template("autoreport_wash.jinja2")

for file in os.listdir(os.path.join(_BASE_PATH)):
    if "wash" in file and file.endswith(".yml"):
        with open(os.path.join(_BASE_PATH, file), "r") as f:
            properties = yaml.safe_load(f)
            file_path = os.path.join(file_dir, f"autoreport_wash_{properties['advertiser']}_{properties['channel']}.py")
            ptbwa_utils.check_result("FILE_PATH", file_path)
            with open(file_path, "w") as python_file:
                python_file.write(template.render(properties))

