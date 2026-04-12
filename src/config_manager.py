import json
import os
from typing import  Dict, Any


def load_config(containing_dir) -> Dict[str,Any]:

    path = os.path.join(containing_dir, "config.json")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data