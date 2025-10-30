# ml/train.py
import os
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score
import pandas as pd
import numpy as np
from ml.utils import load_feature_table, SEED
import joblib

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_URI)
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT", "churn_experiment")
mlflow.set_experiment(EXPERIMENT_NAME)

TARGET = "churned"

def featurize(df):
    if df.empty:
        return df, None
    df = df.copy()
    # Simple feature engineering
    df["plan_basic"] = (df.plan == "basic").astype(int)
    df["plan_premium"] = (df.plan == "premium").astype(int)
    df["device_mobile"] = (df.device == "mobile").astype(int)
    df["signup_days"] = (pd.to_datetime(df.signup_date).dt.date - pd.to_datetime(df.signup_date).dt.date).dt.days # placeholder
    # Using numeric features we have
    features = ["days_since_last_activity", "failed_payments", "unresolved_tickets", "plan_basic", "plan_premium", "device_mobile"]
    X = df[features].fillna(0)
    y = df[TARGET].astype(int) if TARGET in df.columns else None
    return X, y

def train(as_of_date=None):
    df = load_feature_table(as_of_date)
    X, y = featurize(df)
    if X.empty:
        print("No data to train.")
        return None

    # Deterministic split
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=SEED, stratify=y)

    with mlflow.start_run(run_name=f"train_{as_of_date}"):
        clf = RandomForestClassifier(n_estimators=100, random_state=SEED, n_jobs=1)
        clf.fit(X_train, y_train)

        preds = clf.predict_proba(X_val)[:,1]
        auc = roc_auc_score(y_val, preds)
        acc = accuracy_score(y_val, (preds>0.5).astype(int))

        mlflow.log_metric("val_auc", float(auc))
        mlflow.log_metric("val_acc", float(acc))
        mlflow.sklearn.log_model(clf, "model")

        # Register model
        model_name = os.getenv("MODEL_NAME", "churn_model")
        client = mlflow.tracking.MlflowClient()
        run_id = mlflow.active_run().info.run_id
        model_uri = f"runs:/{run_id}/model"
        try:
            client.create_registered_model(model_name)
        except Exception:
            pass
        mv = client.create_model_version(model_name, model_uri, run_id)
        print(f"Registered model version: {mv.version}")

    return {"val_auc": auc, "val_acc": acc, "run_id": run_id, "model_version": mv.version}

if __name__ == "__main__":
    import sys
    as_of = None
    if len(sys.argv) > 1:
        as_of = sys.argv[1]
    res = train(as_of)
    print(res)
