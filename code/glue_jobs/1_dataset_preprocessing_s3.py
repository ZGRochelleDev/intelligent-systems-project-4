"""

Data import and processing Glue job for S3 source data

1. File cleanup / rename bad filenames

2. ETL
    -> read participant folders
    -> process Aware / Oura / Watch / Surveys separately
    -> add metadata
    -> convert surveys to long format
    -> cast schemas consistently
    -> write curated Parquet to S3

"""

import sys
import re
import boto3
from functools import reduce
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


## filename cleanup helpers ##
def sanitize_filename(filename: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", filename)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("._ ")


def split_key(key: str) -> tuple[str, str]:
    if "/" not in key:
        return "", key
    return key.rsplit("/", 1)


def split_name_ext(filename: str) -> tuple[str, str]:
    match = re.match(r"^(.*?)(\.[^.]+)?$", filename)
    if match:
        stem = match.group(1)
        ext = match.group(2) or ""
        return stem, ext
    return filename, ""


def key_exists(bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except s3.exceptions.ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def build_unique_key(bucket_name: str, dir_part: str, filename: str, original_key: str) -> str:
    stem, ext = split_name_ext(filename)

    if dir_part:
        candidate_key = f"{dir_part}/{filename}"
    else:
        candidate_key = filename

    if candidate_key == original_key:
        return candidate_key

    if not key_exists(bucket_name, candidate_key):
        return candidate_key

    counter = 1
    while True:
        candidate_name = f"{stem}_{counter}{ext}"
        if dir_part:
            candidate_key = f"{dir_part}/{candidate_name}"
        else:
            candidate_key = candidate_name

        if candidate_key == original_key:
            return candidate_key

        if not key_exists(bucket_name, candidate_key):
            return candidate_key

        counter += 1


def clean_file_names_in_bucket(bucket: str, prefix: str, dry_run: bool = True):
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            old_key = obj["Key"]

            if not old_key.lower().endswith(".csv"):
                continue

            dir_part, old_filename = split_key(old_key)
            sanitized_filename = sanitize_filename(old_filename)

            if sanitized_filename == old_filename:
                continue

            new_key = build_unique_key(bucket, dir_part, sanitized_filename, old_key)

            print(f"Renaming s3://{bucket}/{old_key} -> s3://{bucket}/{new_key}")

            if not dry_run:
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={"Bucket": bucket, "Key": old_key},
                    Key=new_key
                )
                s3.delete_object(Bucket=bucket, Key=old_key)


## dataframe helpers ##
def union_all(dfs: list[DataFrame]) -> DataFrame | None:
    if not dfs:
        return None
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)


def build_aware_file_paths(participants_to_process: list[str], source_prefix: str, aware_name: str) -> list[str]:
    return [
        f"{source_prefix}/{participant}/Aware/{aware_name}*.csv"
        for participant in participants_to_process
    ]


def build_participant_paths(participants_to_process: list[str], source_prefix: str, subfolder: str = None) -> list[str]:
    if subfolder is None:
        return [f"{source_prefix}/{participant}" for participant in participants_to_process]
    return [f"{source_prefix}/{participant}/{subfolder}" for participant in participants_to_process]


def read_csv_paths(csv_paths: list[str], infer_schema: bool = True) -> DataFrame | None:
    dfs = []

    for csv_path in csv_paths:
        try:
            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", infer_schema)
                .option("encoding", "UTF-8")
                .option("mode", "FAILFAST")
                .csv(csv_path)
                .withColumn("source_file", F.input_file_name())
            )

            # force small read so malformed files fail here
            df.limit(1).collect()
            dfs.append(df)

        except Exception as e:
            print(f"Skipping path: {csv_path}")
            print(e)

    return union_all(dfs)


def add_common_metadata(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "participant_id",
            F.regexp_extract(F.col("source_file"), r"/(Participant_[^/]+)/", 1)
        )
        .withColumn(
            "source_type_raw",
            F.regexp_extract(F.col("source_file"), r"/Participant_[^/]+/([^/]+)/", 1)
        )
        .withColumn("source_type", F.lower(F.col("source_type_raw")))
        .withColumn(
            "file_name",
            F.regexp_extract(F.col("source_file"), r"/([^/]+\.csv)$", 1)
        )
        .withColumn(
            "file_stem",
            F.regexp_replace(F.col("file_name"), r"\.csv$", "")
        )
    )


def write_parquet(dataframe: DataFrame, path: str, write_mode: str = "append", partition_cols=None):
    writer = dataframe.write.mode(write_mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)


## dataset selection helpers ##
def parse_run_datasets(run_datasets_raw: str) -> set[str]:
    items = {
        item.strip().lower()
        for item in run_datasets_raw.split(",")
        if item.strip()
    }
    return items


def should_run(run_datasets: set[str], dataset_name: str) -> bool:
    return "all" in run_datasets or dataset_name.lower() in run_datasets


## aware ##
def process_aware_type(
    participants_to_process: list[str],
    source_prefix: str,
    target_prefix: str,
    aware_name: str,
    write_mode: str = "append"
):
    aware_paths = build_aware_file_paths(participants_to_process, source_prefix, aware_name)
    aware_df = read_csv_paths(aware_paths, infer_schema=True)

    if aware_df is None:
        print(f"No data found for aware/{aware_name}")
        return

    aware_df = (
        aware_df
        .withColumn("source_file", F.input_file_name())
        .withColumn(
            "participant_id",
            F.regexp_extract(F.col("source_file"), r"/(Participant_[^/]+)/", 1)
        )
        .withColumn("source_type_raw", F.lit("Aware"))
        .withColumn("source_type", F.lit("aware"))
        .withColumn(
            "file_name",
            F.regexp_extract(F.col("source_file"), r"/([^/]+\.csv)$", 1)
        )
        .withColumn(
            "file_stem",
            F.regexp_replace(F.col("file_name"), r"\.csv$", "")
        )
        .withColumn("aware_type", F.lit(aware_name))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    if aware_df.head(1):
        print(f"Writing aware/{aware_name} -> {target_prefix}/aware/{aware_name} [{write_mode}]")
        aware_df.printSchema()
        write_parquet(aware_df, f"{target_prefix}/aware/{aware_name}", write_mode=write_mode)


## oura ##
OURA_DOUBLE_COLUMNS = [
    "OURA_activity_average_met",
    "OURA_activity_cal_active",
    "OURA_activity_cal_total",
    "OURA_activity_class_5min",
    "OURA_activity_daily_movement",
    "OURA_activity_high",
    "OURA_activity_inactive",
    "OURA_activity_inactivity_alerts",
    "OURA_activity_low",
    "OURA_activity_medium",
    "OURA_activity_met_1min",
    "OURA_activity_met_min_high",
    "OURA_activity_met_min_inactive",
    "OURA_activity_met_min_low",
    "OURA_activity_met_min_medium",
    "OURA_activity_non_wear",
    "OURA_activity_rest",
    "OURA_activity_rest_mode_state",
    "OURA_activity_score",
    "OURA_activity_score_meet_daily_targets",
    "OURA_activity_score_move_every_hour",
    "OURA_activity_score_recovery_time",
    "OURA_activity_score_stay_active",
    "OURA_activity_score_training_frequency",
    "OURA_activity_score_training_volume",
    "OURA_activity_steps",
    "OURA_activity_target_calories",
    "OURA_activity_target_km",
    "OURA_activity_target_miles",
    "OURA_activity_to_target_km",
    "OURA_activity_to_target_miles",
    "OURA_activity_total",
    "OURA_ideal_bedtime_bedtime_window_end",
    "OURA_ideal_bedtime_bedtime_window_start",
    "OURA_readiness_period_id",
    "OURA_readiness_rest_mode_state",
    "OURA_readiness_score",
    "OURA_readiness_score_activity_balance",
    "OURA_readiness_score_hrv_balance",
    "OURA_readiness_score_previous_day",
    "OURA_readiness_score_previous_night",
    "OURA_readiness_score_recovery_index",
    "OURA_readiness_score_resting_hr",
    "OURA_readiness_score_sleep_balance",
    "OURA_readiness_score_temperature",
    "OURA_sleep_average_breath_variation",
    "OURA_sleep_awake",
    "OURA_sleep_bedtime_end_delta",
    "OURA_sleep_bedtime_start_delta",
    "OURA_sleep_breath_average",
    "OURA_sleep_deep",
    "OURA_sleep_duration",
    "OURA_sleep_efficiency",
    "OURA_sleep_got_up_count",
    "OURA_sleep_hr_5min",
    "OURA_sleep_hr_average",
    "OURA_sleep_hr_lowest",
    "OURA_sleep_hypnogram_5min",
    "OURA_sleep_is_longest",
    "OURA_sleep_light",
    "OURA_sleep_lowest_heart_rate_time_offset",
    "OURA_sleep_midpoint_at_delta",
    "OURA_sleep_midpoint_time",
    "OURA_sleep_onset_latency",
    "OURA_sleep_period_id",
    "OURA_sleep_rem",
    "OURA_sleep_restless",
    "OURA_sleep_rmssd",
    "OURA_sleep_rmssd_5min",
    "OURA_sleep_score",
    "OURA_sleep_score_alignment",
    "OURA_sleep_score_deep",
    "OURA_sleep_score_disturbances",
    "OURA_sleep_score_efficiency",
    "OURA_sleep_score_latency",
    "OURA_sleep_score_rem",
    "OURA_sleep_score_total",
    "OURA_sleep_temperature_delta",
    "OURA_sleep_temperature_deviation",
    "OURA_sleep_temperature_trend_deviation",
    "OURA_sleep_total",
    "OURA_sleep_wake_up_count",
]


def cast_oura_columns(oura_df: DataFrame) -> DataFrame:
    if "timestamp" in oura_df.columns:
        oura_df = oura_df.withColumn("timestamp", F.col("timestamp").cast("long"))

    if "participant" in oura_df.columns:
        oura_df = oura_df.withColumn("participant", F.col("participant").cast("string"))

    for col_name in OURA_DOUBLE_COLUMNS:
        if col_name in oura_df.columns:
            oura_df = oura_df.withColumn(col_name, F.col(col_name).cast("double"))

    return oura_df


def process_oura(
    participants_to_process: list[str],
    source_prefix: str,
    target_prefix: str,
    write_mode: str = "append"
):
    oura_paths = build_participant_paths(participants_to_process, source_prefix, "Oura")
    oura_df = read_csv_paths(oura_paths, infer_schema=True)

    if oura_df is None:
        print("No Oura data found")
        return

    oura_df = cast_oura_columns(oura_df)

    oura_df = (
        add_common_metadata(oura_df)
        .withColumn("ingest_ts", F.current_timestamp())
    )

    if oura_df.head(1):
        print(f"Writing oura -> {target_prefix}/oura [{write_mode}]")
        oura_df.printSchema()
        write_parquet(oura_df, f"{target_prefix}/oura", write_mode=write_mode)


## watch ##
def process_watch(
    participants_to_process: list[str],
    source_prefix: str,
    target_prefix: str,
    write_mode: str = "append"
):
    watch_paths = build_participant_paths(participants_to_process, source_prefix, "Watch")
    watch_df = read_csv_paths(watch_paths, infer_schema=True)

    if watch_df is None:
        print("No Watch data found")
        return

    watch_df = (
        add_common_metadata(watch_df)
        .withColumn("ingest_ts", F.current_timestamp())
    )

    if watch_df.head(1):
        print(f"Writing watch -> {target_prefix}/watch [{write_mode}]")
        watch_df.printSchema()
        write_parquet(watch_df, f"{target_prefix}/watch", write_mode=write_mode)


## surveys ##
def process_surveys(
    participants_to_process: list[str],
    source_prefix: str,
    target_prefix: str,
    write_mode: str = "append"
):
    survey_paths = build_participant_paths(participants_to_process, source_prefix, "Surveys")

    survey_file_paths = []

    for survey_dir in survey_paths:
        try:
            no_scheme = survey_dir.replace("s3://", "", 1)
            bucket_name, key_prefix = no_scheme.split("/", 1)

            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=key_prefix.rstrip("/") + "/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.lower().endswith(".csv"):
                        survey_file_paths.append(f"s3://{bucket_name}/{key}")

        except Exception as e:
            print(f"Skipping survey directory: {survey_dir}")
            print(e)

    if not survey_file_paths:
        print("No Survey data found")
        return

    surveys_df = read_csv_paths(survey_file_paths, infer_schema=True)

    if surveys_df is None:
        print("No Survey data could be read successfully")
        return

    surveys_df = add_common_metadata(surveys_df)
    survey_columns = surveys_df.columns

    metadata_cols = {
        "participant_id",
        "participant",
        "source_file",
        "source_type_raw",
        "source_type",
        "file_name",
        "file_stem"
    }

    for maybe_col in ["date", "Date", "timestamp", "Timestamp", "datetime", "Datetime"]:
        if maybe_col in survey_columns:
            metadata_cols.add(maybe_col)

    question_cols = [
        c for c in survey_columns
        if c not in metadata_cols and c.lower().startswith("q")
    ]

    for c in question_cols:
        surveys_df = surveys_df.withColumn(c, F.col(c).cast("string"))

    if not question_cols:
        print("No survey question columns found")
        return

    stack_expr = "stack({0}, {1}) as (question_id, response_raw)".format(
        len(question_cols),
        ", ".join([f"'{c}', `{c}`" for c in question_cols])
    )

    surveys_long = (
        surveys_df
        .select(
            *[c for c in surveys_df.columns if c in metadata_cols],
            F.expr(stack_expr)
        )
        .withColumn("survey_source_file", F.col("file_name"))
        .withColumn("survey_path", F.col("source_file"))
        .withColumn("survey_type", F.lit(None).cast("string"))
        .withColumn("question_text", F.lit(None).cast("string"))
        .withColumn("response_raw", F.col("response_raw").cast("string"))
        .withColumn("response_numeric", F.lit(None).cast("double"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    surveys_long = surveys_long.filter(
        F.col("response_raw").isNotNull() & (F.trim(F.col("response_raw")) != "")
    )

    if surveys_long.head(1):
        print(f"Writing surveys_long -> {target_prefix}/surveys_long [{write_mode}]")
        surveys_long.printSchema()
        write_parquet(surveys_long, f"{target_prefix}/surveys_long", write_mode=write_mode)


## orchestration ##
def iterate_participant_folders_s3(
    participants_to_process=None,
    source_prefix=None,
    target_prefix=None,
    run_datasets=None,
    write_mode="append"
):
    aware_names = ["battery", "calls", "screen", "messages", "notifications"]

    for aware_name in aware_names:
        if should_run(run_datasets, f"aware:{aware_name}"):
            process_aware_type(
                participants_to_process,
                source_prefix,
                target_prefix,
                aware_name,
                write_mode=write_mode
            )

    if should_run(run_datasets, "oura"):
        process_oura(participants_to_process, source_prefix, target_prefix, write_mode=write_mode)

    if should_run(run_datasets, "watch"):
        process_watch(participants_to_process, source_prefix, target_prefix, write_mode=write_mode)

    if should_run(run_datasets, "surveys"):
        process_surveys(participants_to_process, source_prefix, target_prefix, write_mode=write_mode)

    job.commit()


## main ##
if __name__ == "__main__":
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "SOURCE_PREFIX",
            "TARGET_PREFIX",
            "BUCKET",
            "PREFIX",
            "DRY_RUN",
            "RUN_DATASETS",
            "WRITE_MODE",
        ]
    )

    s3 = boto3.client("s3")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    source_prefix = args["SOURCE_PREFIX"].rstrip("/")
    target_prefix = args["TARGET_PREFIX"].rstrip("/")
    bucket = args["BUCKET"]
    prefix = args["PREFIX"].rstrip("/") + "/"
    dry_run = args["DRY_RUN"].lower() == "true"
    run_datasets = parse_run_datasets(args["RUN_DATASETS"])
    write_mode = args["WRITE_MODE"].lower()

    # clean_file_names_in_bucket(bucket, prefix, dry_run)

    participants_to_process = [
        "Participant_1",
        "Participant_18",
        "Participant_25",
        "Participant_36",
        "Participant_45",
        "Participant_10",
        "Participant_19",
        "Participant_26",
        "Participant_38",
        "Participant_46",
        "Participant_11",
        "Participant_2",
        "Participant_27",
        "Participant_39",
        "Participant_5",
        "Participant_12",
        "Participant_20",
        "Participant_28",
        "Participant_40",
        "Participant_6",
        "Participant_13",
        "Participant_21",
        "Participant_29",
        "Participant_41",
        "Participant_7",
        "Participant_15",
        "Participant_22",
        "Participant_3",
        "Participant_42",
        "Participant_8",
        "Participant_16",
        "Participant_23",
        "Participant_31",
        "Participant_43",
        "Participant_9",
        "Participant_17",
        "Participant_24",
        "Participant_32",
        "Participant_44"
    ]

    iterate_participant_folders_s3(
        participants_to_process=participants_to_process,
        source_prefix=source_prefix,
        target_prefix=target_prefix,
        run_datasets=run_datasets,
        write_mode=write_mode
    )
