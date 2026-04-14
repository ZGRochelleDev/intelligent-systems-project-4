"""

Feature extraction and survey scoring Glue job
    Features: participants' behavior and wearable data
    Loneliness: determined by UCLA survey responses

This code does the following:
    1. Builds the target labels
        by using the UCLA survey responses
        to score each participant's loneliness: Never -> 1, Rarely -> 2, etc.
        then converts that score into a binary label:
        - high_loneliness
        - low_loneliness

    2. Builds participant-level features (1 participant per row)
        queries the Aware and Oura Athena tables
        aggregates that data into one row per participant
        merges those features with the UCLA labels

    3. Outputs a dataframe "model_df" that can be used in the later training script.

"""

import awswrangler as wr
import pandas as pd
import numpy as np

DATABASE = "loneliness_dataset"
WORKGROUP = "primary" 

## configuration ##
# - Example response mapping for a 4-point UCLA-style response set.
RESPONSE_MAP = {
    "Never": 1,
    "Rarely": 2,
    "Sometimes": 3,
    "Often": 4,
    "never": 1,
    "rarely": 2,
    "sometimes": 3,
    "often": 4,
}

REVERSE_CODED_ITEMS = [
    "q1", "q5", "q6", "q9", "q10",
    "q15", "q16", "q19", "q20"
]

MIN_SCORE = 1
MAX_SCORE = 4


## step 1: read UCLA survey rows from Athena ##
sql = """
SELECT
    participant,
    date,
    timestamp,
    file_name,
    file_stem,
    question_id,
    response_raw
FROM loneliness_dataset.surveys_long
WHERE file_stem LIKE '%UCLA%'
"""

ucla_long = wr.athena.read_sql_query(
    sql=sql,
    database=DATABASE,
    workgroup=WORKGROUP,
    ctas_approach=False
)

print("Raw UCLA rows:")
print(ucla_long.head())


## step 2: if a participant has multiple UCLA survey instances, keep the latest one ##
ucla_long = ucla_long.sort_values(["participant", "timestamp"])

latest_survey_ts = (
    ucla_long.groupby("participant", as_index=False)["timestamp"]
    .max()
    .rename(columns={"timestamp": "latest_timestamp"})
)

ucla_long = ucla_long.merge(
    latest_survey_ts,
    on="participant",
    how="inner"
)

ucla_long = ucla_long[ucla_long["timestamp"] == ucla_long["latest_timestamp"]].copy()
ucla_long.drop(columns=["latest_timestamp"], inplace=True)

print("\nLatest UCLA rows only:")
print(ucla_long.head())


## step 3: pivot from long format to one row per participant ##
ucla_wide = (
    ucla_long.pivot_table(
        index=["participant", "date", "timestamp", "file_name", "file_stem"],
        columns="question_id",
        values="response_raw",
        aggfunc="first"
    )
    .reset_index()
)

# flatten pivoted columns
ucla_wide.columns.name = None

print("\nWide UCLA table:")
print(ucla_wide.head())


## step 4: identify UCLA question columns ##
question_cols = sorted(
    [c for c in ucla_wide.columns if c.lower().startswith("q")],
    key=lambda x: int(x[1:]) if x[1:].isdigit() else x
)

print("\nQuestion columns:", question_cols)


## step 5: convert text responses to numeric ##
for col in question_cols:
    ucla_wide[col] = ucla_wide[col].map(RESPONSE_MAP)

print("\nAfter response mapping:")
print(ucla_wide[["participant"] + question_cols].head())



## step 6: reverse-score selected items ##
# new_score = (MAX_SCORE + MIN_SCORE) - old_score
# for a 1-4 scale, this is 5 - old_score
for col in REVERSE_CODED_ITEMS:
    if col in ucla_wide.columns:
        ucla_wide[col] = ucla_wide[col].apply(
            lambda x: (MAX_SCORE + MIN_SCORE) - x if pd.notnull(x) else np.nan
        )

print("\nAfter reverse scoring:")
print(ucla_wide[["participant"] + question_cols].head())


## step 7: compute total UCLA score per participant ##
ucla_wide["ucla_total_score"] = ucla_wide[question_cols].sum(axis=1, skipna=False)

# Require all items to be present
ucla_scored = ucla_wide.dropna(subset=question_cols).copy()

print("\nScored UCLA data:")
print(ucla_scored[["participant", "ucla_total_score"]].head())



## step 8: compute median split ##
# high loneliness = score > median
# low loneliness = score <= median
median_score = ucla_scored["ucla_total_score"].median()

ucla_scored["loneliness_label"] = np.where(
    ucla_scored["ucla_total_score"] > median_score,
    "high_loneliness",
    "low_loneliness"
)

ucla_scored["loneliness_label_binary"] = np.where(
    ucla_scored["ucla_total_score"] > median_score,
    1,
    0
)

print(f"\nMedian UCLA score: {median_score}")
print(ucla_scored[["participant", "ucla_total_score", "loneliness_label", "loneliness_label_binary"]])


## final output: one row per participant ##
participant_labels = ucla_scored[
    [
        "participant",
        "date",
        "timestamp",
        "file_name",
        "file_stem",
        "ucla_total_score",
        "loneliness_label",
        "loneliness_label_binary"
    ]
].copy()

print("\nFinal participant-level label table:")
print(participant_labels.head())


## Build the participant-level modeling dataset ##
# Use the one label row per participant dataframe to build one feature row per participant from the Athena tables, then merge everything together.
def run_athena_query(sql: str) -> pd.DataFrame:
    return wr.athena.read_sql_query(
        sql=sql,
        database=DATABASE,
        workgroup=WORKGROUP,
        ctas_approach=False
    )



## 1. AWARE FEATURES ##
battery_sql = """
SELECT
    participant,
    AVG(CAST(battery_charge_start AS double)) AS avg_battery_charge_start,
    AVG(CAST(battery_charge_end AS double)) AS avg_battery_charge_end,
    COUNT(*) AS battery_event_count
FROM loneliness_dataset.aware_battery
GROUP BY participant
"""

calls_sql = """
SELECT
    participant,
    COUNT(*) AS total_calls,
    AVG(CAST(dur AS double)) AS avg_call_duration,
    SUM(CAST(dur AS double)) AS total_call_duration,
    AVG(CAST(type AS double)) AS avg_call_type
FROM loneliness_dataset.aware_calls
GROUP BY participant
"""

messages_sql = """
SELECT
    participant,
    COUNT(*) AS total_messages,
    AVG(CAST(message_type AS double)) AS avg_message_type
FROM loneliness_dataset.aware_messages
GROUP BY participant
"""

notifications_sql = """
SELECT
    participant,
    COUNT(*) AS total_notifications
FROM loneliness_dataset.aware_notifications
GROUP BY participant
"""

screen_sql = """
SELECT
    participant,
    COUNT(*) AS total_screen_events,
    AVG(CAST(screen_status AS double)) AS avg_screen_status
FROM loneliness_dataset.aware_screen
GROUP BY participant
"""

battery_df = run_athena_query(battery_sql)
calls_df = run_athena_query(calls_sql)
messages_df = run_athena_query(messages_sql)
notifications_df = run_athena_query(notifications_sql)
screen_df = run_athena_query(screen_sql)

aware_features_df = battery_df.merge(calls_df, on="participant", how="outer")
aware_features_df = aware_features_df.merge(messages_df, on="participant", how="outer")
aware_features_df = aware_features_df.merge(notifications_df, on="participant", how="outer")
aware_features_df = aware_features_df.merge(screen_df, on="participant", how="outer")

print("Aware features:")
print(aware_features_df.head())


## 2. OURA FEATURES ##
oura_sql = """
SELECT
    participant,

    AVG(OURA_sleep_duration) AS avg_sleep_duration,
    AVG(OURA_sleep_score) AS avg_sleep_score,
    AVG(OURA_sleep_total) AS avg_sleep_total,
    AVG(OURA_sleep_efficiency) AS avg_sleep_efficiency,
    AVG(OURA_sleep_rem) AS avg_sleep_rem,
    AVG(OURA_sleep_deep) AS avg_sleep_deep,
    AVG(OURA_sleep_light) AS avg_sleep_light,
    AVG(OURA_sleep_awake) AS avg_sleep_awake,

    AVG(OURA_activity_steps) AS avg_steps,
    AVG(OURA_activity_score) AS avg_activity_score,
    AVG(OURA_activity_total) AS avg_activity_total,
    AVG(OURA_activity_daily_movement) AS avg_daily_movement,
    AVG(OURA_activity_average_met) AS avg_average_met,

    AVG(OURA_readiness_score) AS avg_readiness_score,
    AVG(OURA_readiness_score_activity_balance) AS avg_readiness_activity_balance,
    AVG(OURA_readiness_score_hrv_balance) AS avg_readiness_hrv_balance,
    AVG(OURA_readiness_score_resting_hr) AS avg_readiness_resting_hr,
    AVG(OURA_readiness_score_sleep_balance) AS avg_readiness_sleep_balance,

    AVG(OURA_sleep_hr_average) AS avg_sleep_hr_average,
    AVG(OURA_sleep_hr_lowest) AS avg_sleep_hr_lowest,
    AVG(OURA_sleep_rmssd) AS avg_sleep_rmssd,
    AVG(OURA_sleep_average_breath_variation) AS avg_breath_variation,

    COUNT(*) AS oura_record_count
FROM loneliness_dataset.oura
GROUP BY participant
"""

oura_features_df = run_athena_query(oura_sql)

print("Oura features:")
print(oura_features_df.head())


## 3. LABELS ##
# Required columns:
# participant, ucla_total_score, loneliness_label, loneliness_label_binary

labels_df = participant_labels.copy()

print("Labels:")
print(labels_df.head())


## 4. MERGE INTO FINAL MODELING DATASET ##
participant_features_df = aware_features_df.merge(
    oura_features_df,
    on="participant",
    how="outer"
)

model_df = participant_features_df.merge(
    labels_df[["participant", "ucla_total_score", "loneliness_label", "loneliness_label_binary"]],
    on="participant",
    how="inner"
)

print("Final modeling dataset:")
print(model_df.head())
print(model_df.shape)


## 5. BASIC CLEANUP ##
# Fill missing numeric feature values with column medians
feature_cols = [
    c for c in model_df.columns
    if c not in ["participant", "ucla_total_score", "loneliness_label", "loneliness_label_binary"]
]

for col in feature_cols:
    if pd.api.types.is_numeric_dtype(model_df[col]):
        model_df[col] = model_df[col].astype("float64")
        model_df[col] = model_df[col].fillna(model_df[col].median())

print("Cleaned modeling dataset:")
print(model_df.head())

## write out to s3 to be used in the training job ##
wr.s3.to_parquet(
    df=model_df,
    path="s3://zgr2020-bucket/quinnipiac-data/dataframes/model_df.parquet",
    index=False,
    dataset=False
)
