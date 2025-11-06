-- Create database
CREATE DATABASE IF NOT EXISTS video_analytics;

-- Use the database
USE video_analytics;

-- Create table for video logs
CREATE TABLE IF NOT EXISTS video_logs
(
    original_id String,
    original_timestamp String,
    video_id String,
    session_id String,
    watched_seconds UInt32,
    video_duration_seconds UInt32,
    watched_ratio Float64,
    device_type String,
    quality String,
    ingestion_time DateTime DEFAULT now(),
    is_deleted Bool
)
ENGINE = ReplacingMergeTree()
ORDER BY (original_id, video_id, session_id)  -- Deduplication key
PRIMARY KEY (original_id);