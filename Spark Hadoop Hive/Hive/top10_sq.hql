--import data from csv and create table;
DROP TABLE resultEs2;
DROP TABLE reviews;

CREATE TABLE IF NOT EXISTS reviews (
	id STRING,
	productId STRING,
	userId STRING,
	profileName STRING,
	hNum STRING,
	hDen STRING,
	score STRING,
	time STRING,
	summary STRING,
	text STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '${INPUT_DIR}' INTO TABLE reviews;

--add jar for converting unix timestamp;
add jar ${CUSTOM_JAR_PATH};
CREATE TEMPORARY FUNCTION ranking AS 'it.uniroma3.kucha.Rank';

CREATE TABLE resultEs2 AS
SELECT ps.userId, ps.productId, ps.avg FROM (
	SELECT userId, productId, ROUND(AVG(CAST(score AS FLOAT)),3) AS avg FROM reviews GROUP BY userId, productId
	) ps
	ORDER BY ps.userId, ps.avg DESC;

SELECT userId, productId, avg FROM resultES2 WHERE ranking(userId,avg) < 10;




