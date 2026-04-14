"""

Feature Evaluation job

Evaluate feature importance using Random Forest and XGBoost models to identify which features are most predictive of loneliness.
This will help us understand which aspects of the data are most relevant for predicting loneliness and can inform future feature engineering efforts.

We need to have one final fitted model to get feature importance, so we will fit both a Random Forest and an XGBoost model on the full dataset (without cross-validation) to extract feature importance scores. We will then compare the top features from both models.

"""

import awswrangler as wr
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier


## Read in the modeling dataset that was built in the previous step: 3_begin_training_dataset.py ##
X = model_df.drop(columns=[
    "participant",
    "ucla_total_score",
    "loneliness_label",
    "loneliness_label_binary"
]).copy()

y = model_df["loneliness_label_binary"].copy()


## Random Forest feature importance ##
rf_pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("model", RandomForestClassifier(
        n_estimators=200,
        random_state=42
    ))
])

rf_pipeline.fit(X, y)
rf_model = rf_pipeline.named_steps["model"]
rf_feature_importance_df = pd.DataFrame({
    "feature": X.columns,
    "importance": rf_model.feature_importances_
}).sort_values(by="importance", ascending=False)

print("Random Forest feature importance:")
print(rf_feature_importance_df.head(15))


## XGBoost feature importance ##
xgb_pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("model", XGBClassifier(
        n_estimators=200,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        eval_metric="logloss",
        random_state=42
    ))
])

xgb_pipeline.fit(X, y)
xgb_model = xgb_pipeline.named_steps["model"]
xgb_feature_importance_df = pd.DataFrame({
    "feature": X.columns,
    "importance": xgb_model.feature_importances_
}).sort_values(by="importance", ascending=False)

print("XGBoost feature importance:")
print(xgb_feature_importance_df.head(15))


## Plotting Random Forest feature importance ##
top_rf = rf_feature_importance_df.head(10).sort_values("importance")

plt.figure(figsize=(8, 6))
plt.barh(top_rf["feature"], top_rf["importance"])
plt.xlabel("Importance")
plt.ylabel("Feature")
plt.title("Top 10 Random Forest Feature Importances")
plt.tight_layout()
plt.show()


## Plotting XGBoost feature importance ##
top_xgb = xgb_feature_importance_df.head(10).sort_values("importance")

plt.figure(figsize=(8, 6))
plt.barh(top_xgb["feature"], top_xgb["importance"])
plt.xlabel("Importance")
plt.ylabel("Feature")
plt.title("Top 10 XGBoost Feature Importances")
plt.tight_layout()
plt.show()


## Save feature importance results to S3 ##
wr.s3.to_csv(
    df=rf_feature_importance_df,
    path="s3://zgr2020-bucket/quinnipiac-data/results/rf_feature_importance.csv",
    index=False
)

wr.s3.to_csv(
    df=xgb_feature_importance_df,
    path="s3://zgr2020-bucket/quinnipiac-data/results/xgb_feature_importance.csv",
    index=False
)
