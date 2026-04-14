/* DDL - for AWS:Athena database */

CREATE DATABASE IF NOT EXISTS loneliness_curated;


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.surveys_long (
    participant string,
    date string,
    timestamp string,
    survey_source_file string,
    survey_path string,
    survey_type string,
    question_id string,
    question_text string,
    response_raw string,
    response_numeric double,
    ingest_ts timestamp
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/surveys_long/';


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.oura (
    participant string,
    timestamp string,
    OURA_activity_average_met double,
    OURA_activity_cal_active double,
    OURA_activity_cal_total double,
    OURA_activity_class_5min string,
    OURA_activity_daily_movement double,
    OURA_activity_high double,
    OURA_activity_inactive double,
    OURA_activity_inactivity_alerts double,
    OURA_activity_low double,
    OURA_activity_medium double,
    OURA_activity_met_1min string,
    OURA_activity_met_min_high double,
    OURA_activity_met_min_inactive double,
    OURA_activity_met_min_low double,
    OURA_activity_met_min_medium double,
    OURA_activity_non_wear double,
    OURA_activity_rest double,
    OURA_activity_rest_mode_state string,
    OURA_activity_score double,
    OURA_activity_score_meet_daily_targets double,
    OURA_activity_score_move_every_hour double,
    OURA_activity_score_recovery_time double,
    OURA_activity_score_stay_active double,
    OURA_activity_score_training_frequency double,
    OURA_activity_score_training_volume double,
    OURA_activity_steps double,
    OURA_activity_target_calories double,
    OURA_activity_target_km double,
    OURA_activity_target_miles double,
    OURA_activity_to_target_km double,
    OURA_activity_to_target_miles double,
    OURA_activity_total double,
    OURA_ideal_bedtime_bedtime_window_end string,
    OURA_ideal_bedtime_bedtime_window_start string,
    OURA_readiness_period_id string,
    OURA_readiness_rest_mode_state string,
    OURA_readiness_score double,
    OURA_readiness_score_activity_balance double,
    OURA_readiness_score_hrv_balance double,
    OURA_readiness_score_previous_day double,
    OURA_readiness_score_previous_night double,
    OURA_readiness_score_recovery_index double,
    OURA_readiness_score_resting_hr double,
    OURA_readiness_score_sleep_balance double,
    OURA_readiness_score_temperature double,
    OURA_sleep_average_breath_variation double,
    OURA_sleep_awake double,
    OURA_sleep_bedtime_end_delta double,
    OURA_sleep_bedtime_start_delta double,
    OURA_sleep_breath_average double,
    OURA_sleep_deep double,
    OURA_sleep_duration double,
    OURA_sleep_efficiency double,
    OURA_sleep_got_up_count double,
    OURA_sleep_hr_5min string,
    OURA_sleep_hr_average double,
    OURA_sleep_hr_lowest double,
    OURA_sleep_hypnogram_5min string,
    OURA_sleep_is_longest boolean,
    OURA_sleep_light double,
    OURA_sleep_lowest_heart_rate_time_offset double,
    OURA_sleep_midpoint_at_delta double,
    OURA_sleep_midpoint_time string,
    OURA_sleep_onset_latency double,
    OURA_sleep_period_id string,
    OURA_sleep_rem double,
    OURA_sleep_restless double,
    OURA_sleep_rmssd double,
    OURA_sleep_rmssd_5min string,
    OURA_sleep_score double,
    OURA_sleep_score_alignment double,
    OURA_sleep_score_deep double,
    OURA_sleep_score_disturbances double,
    OURA_sleep_score_efficiency double,
    OURA_sleep_score_latency double,
    OURA_sleep_score_rem double,
    OURA_sleep_score_total double,
    OURA_sleep_temperature_delta double,
    OURA_sleep_temperature_deviation double,
    OURA_sleep_temperature_trend_deviation double,
    OURA_sleep_total double,
    OURA_sleep_wake_up_count double
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/oura/';


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.watch (
    timestamp string,
    ppg string,
    hrm double,
    accx double,
    accy double,
    accz double,
    grax double,
    gray double,
    graz double,
    gyrx double,
    gyry double,
    gyrz double,
    pressure double
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/watch/';


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.aware_battery (
    timestamp string,
    participant string,
    battery_charge_start double,
    battery_charge_end double
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/aware/battery/';


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.aware_calls (
    timestamp string,
    participant string,
    dur double,
    type string
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/aware/calls/';


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.aware_messages (
    timestamp string,
    participant string,
    message_type string
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/aware/messages/';


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.aware_notifications (
    timestamp string,
    participant string,
    package_category string
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/aware/notifications/';


CREATE EXTERNAL TABLE IF NOT EXISTS loneliness_dataset.aware_screen (
    timestamp string,
    participant string,
    screen_status string
)
STORED AS PARQUET
LOCATION 's3://zgr2020-bucket/quinnipiac-data/curated/aware/screen/';
