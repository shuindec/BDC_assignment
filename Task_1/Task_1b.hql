DROP TABLE bankdata;
DROP TABLE avg_balance;

-- Create a table for the input data
CREATE TABLE bankdata (age BIGINT, job STRING, marital STRING, education STRING,
  default STRING, balance BIGINT, housing STRING, loan STRING, contact STRING,
  day BIGINT, month STRING, duration BIGINT, campaign BIGINT, pdays BIGINT,
  previous BIGINT, poutcome STRING, termdeposit STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/bank.csv' INTO TABLE bankdata;

-- TODO: *** Put your solution here ***
-- Create table to select and calculate the avg balance of each education category

CREATE TABLE avg_balance AS
SELECT education, avg(balance) 
FROM bankdata
GROUP BY education;

-- Dump data to the output file
INSERT OVERWRITE LOCAL DIRECTORY './task_1b-out/'
	SELECT * FROM avg_balance;
