# Guided Capstone

Goal: Build a data pipeline to ingest and process daily stock market data form multiple stock exchanges, to produce a dataset including the below indicators for business analysts.
- Latest trade price
- Prior day closing price
- 30-min moving average trade price

## 1/ Database table design
- Host the trade and quote data
- Ensure the data growth over time doesn’t impact query performance

## 2/ Data ingestion
- Ingest source data from JSON or CSV files
- Drop records that do not follow the schema

## 3/ EOD (End of Day) batch load
- Load all the progressively processed data for current day into daily tables for trade and
quote
- Schedule run at 5pm every day

## 4/ Analytical ETL job
- Calculate supplemental information for quote records:
    - The latest trade price before the quote
    - The latest 30 min moving average trade price before the quote
    - The bid and ask price movement (difference) from the previous day's last trade price

## 5/ Pipeline orchestration
- Design one or multiple workflows to execute the individual job
- Maintain a job status table to keep track of running progress of each workflow
- Support workflow/job rerun in case of failure while running the job