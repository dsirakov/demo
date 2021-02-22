CREATE DATABASE IF NOT EXISTS viademo;

CREATE SCHEMA IF NOT EXISTS dsirakov;

CREATE TABLE IF NOT EXISTS dsirakov.tlc_applications (
    app_no BIGINT,
    type TEXT,
    app_date TIMESTAMP,
    status TEXT,
    fru_interview_scheduled TEXT,
    drug_test TEXT,
    wav_course TEXT,
    defensive_driving TEXT,
    driver_exam TEXT,
    medical_clearance TEXT,	
    other_requirements TEXT,
    last_updated TIMESTAMP,
    source_id TEXT
);