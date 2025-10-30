# ml/evaluate.py
import os
import mlflow
import pandas as pd
from ml.utils import load_feature_table
from ml.train import featurize
from mlflow.tracking import MlflowClient
from sklearn.metrics import roc_auc_score
import numpy as np

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_URI)
client = MlflowClient(MLFLOW_URI)
MODEL_NAME = os.getenv("MODEL_NAME", "churn_model")
THRESHOLD = float(os.getenv("EVAL_AUC_THRESHOLD", 0.70))

def evaluate_latest_model(as_of_date=None):
    df = load_feature_table(as_of_date)
    X, y = featurize(df)
    if X.empty:
        print("No data to evaluate.")
        return {"auc": None, "ok": False}

    # get latest model version in 'Production' or latest registered version
    versions = client.get_latest_versions(MODEL_NAME)
    if not versions:
        print("No model registered yet.")
        return {"auc": None, "ok": False}

    # pick the highest version number
    latest = sorted(versions, key=lambda v: int(v.version))[-1]
    model_uri = f"models:/{MODEL_NAME}/{latest.version}"
    model = mlflow.sklearn.load_model(model_uri)

    preds = model.predict_proba(X)[:,1]
    auc = roc_auc_score(y, preds)

    ok = auc >= THRESHOLD
    print(f"AUC={auc:.4f} threshold={THRESHOLD} OK={ok}")
    return {"auc": float(auc), "ok": ok, "model_version": latest.version}

if __name__ == "__main__":
    import sys
    as_of = None
    if len(sys.argv) > 1:
        as_of = sys.argv[1]
    print(evaluate_latest_model(as_of))
