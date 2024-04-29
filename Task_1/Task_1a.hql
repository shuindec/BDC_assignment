DROP TABLE bankdata;

DROP TABLE job_small;

-- Create a table for the input data
CREATE TABLE bankdata (age BIGINT, job STRING, marital STRING, education STRING,
  default STRING, balance BIGINT, housing STRING, loan STRING, contact STRING,
  day BIGINT, month STRING, duration BIGINT, campaign BIGINT, pdays BIGINT,
  previous BIGINT, poutcome STRING, termdeposit STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/bank.csv' INTO TABLE bankdata;

-- TODO: *** Put your solution here ***
-- Create a table with cleaned data for job categories

CREATE TABLE job_small AS
SELECT job, count(job) AS count
FROM bankdata
GROUP BY job;

-- Dump the output to the file
INSERT OVERWRITE LOCAL DIRECTORY './task_1a-out/'
  SELECT * from job_small;



