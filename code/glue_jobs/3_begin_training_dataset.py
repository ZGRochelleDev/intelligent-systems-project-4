"""

Training Dataset and Machine learning job

This script performs the initial training of various machine learning models
(SVM, Random Forest, XGBoost, Neural Network) using the prepared modeling
dataset. It includes cross-validation to evaluate model performance across
multiple metrics.

 - model_df = the full dataset with all features and labels, read in from S3
 - X = the feature matrix (all columns except participant, scores, and labels)
 - y = the target variable (binary loneliness label)

"""

import pandas as pd
import numpy as np
import awswrangler as wr

from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier

from xgboost import XGBClassifier

## read in the modeling dataset that was built in the previous step ##
model_df = wr.s3.read_parquet(
    path="s3://zgr2020-bucket/quinnipiac-data/dataframes/model_df.parquet"
)


## 1. Define X and y ##
X = model_df.drop(columns=[
    "participant",
    "ucla_total_score",
    "loneliness_label",
    "loneliness_label_binary"
]).copy()

y = model_df["loneliness_label_binary"].copy()
print("X shape:", X.shape)
print("y shape:", y.shape)
print("Class distribution:")
print(y.value_counts())


## 2. Cross-validation setup ##
cv = StratifiedKFold(
    n_splits=5,
    shuffle=True,
    random_state=42
)

scoring = {
    "accuracy": "accuracy",
    "precision": "precision",
    "recall": "recall",
    "f1": "f1",
    "roc_auc": "roc_auc"
}


## 3. Define models ##
models = {
    "SVM": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("model", SVC(
            kernel="rbf",
            C=1.0,
            probability=True,
            random_state=42
        ))
    ]),

    "Random Forest": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", RandomForestClassifier(
            n_estimators=200,
            max_depth=None,
            min_samples_split=2,
            min_samples_leaf=1,
            random_state=42
        ))
    ]),

    "XGBoost": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("model", XGBClassifier(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            eval_metric="logloss",
            use_label_encoder=False,
            random_state=42
        ))
    ]),

    "Neural Network": Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("model", MLPClassifier(
            hidden_layer_sizes=(32, 16),
            activation="relu",
            solver="adam",
            alpha=0.0001,
            max_iter=1000,
            random_state=42
        ))
    ])
}


## 4. Train + evaluate with cross-validation ##
results = []

for model_name, model in models.items():
    print(f"\nRunning cross-validation for: {model_name}")

    scores = cross_validate(
        estimator=model,
        X=X,
        y=y,
        cv=cv,
        scoring=scoring,
        return_train_score=False,
        n_jobs=-1
    )

    model_result = {
        "model": model_name,
        "accuracy_mean": scores["test_accuracy"].mean(),
        "accuracy_std": scores["test_accuracy"].std(),

        "precision_mean": scores["test_precision"].mean(),
        "precision_std": scores["test_precision"].std(),

        "recall_mean": scores["test_recall"].mean(),
        "recall_std": scores["test_recall"].std(),

        "f1_mean": scores["test_f1"].mean(),
        "f1_std": scores["test_f1"].std(),

        "roc_auc_mean": scores["test_roc_auc"].mean(),
        "roc_auc_std": scores["test_roc_auc"].std()
    }

    results.append(model_result)

results_df = pd.DataFrame(results)

## Sort by F1 score first, since that's often a strong summary metric ##
results_df = results_df.sort_values(by="f1_mean", ascending=False).reset_index(drop=True)

print("\nCross-validation results:")
print(results_df)

## Round for easier reading ##
results_df_rounded = results_df.copy()
metric_cols = [col for col in results_df_rounded.columns if col != "model"]
results_df_rounded[metric_cols] = results_df_rounded[metric_cols].round(4)

print("\nRounded results:")
print(results_df_rounded)
