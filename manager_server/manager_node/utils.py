# manager_node/utils.py
import logging
from pathlib import Path
import xgboost as xgb

def setup_logger(name, log_path: Path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_path, mode='w')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def load_xgboost_model(model_path):
    xgboost_proc_model = xgb.Booster()
    xgboost_proc_model.load_model(model_path)

    print("Xgboost model started")
    return xgboost_proc_model