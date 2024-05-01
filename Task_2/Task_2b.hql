DROP TABLE twitterdata;
DROP TABLE result;

-- Create a table for the input data
CREATE TABLE twitterdata (tokenType STRING, months STRING, counts BIGINT,
  hashtagName STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

-- Load the input data
LOAD DATA LOCAL INPATH 'Data/twitter.tsv' INTO TABLE twitterdata;

-- TODO: *** Put your solution here ***

CREATE TABLE result AS
SELECT t1.hashtagname, t1.count_sum FROM (
    SELECT *, row_number() over (order by count_sum desc) rn
    FROM (SELECT hashtagname, sum(counts) as count_sum
          FROM twitterdata GROUP BY hashtagname SORT BY count_sum desc) a
)t1
WHERE rn = 1;

-- Save data to the output file
INSERT OVERWRITE LOCAL DIRECTORY './task_2b-out/'
    SELECT * FROM result;