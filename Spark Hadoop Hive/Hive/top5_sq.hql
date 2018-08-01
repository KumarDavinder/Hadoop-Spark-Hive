--import data from csv and create table;
DROP TABLE resultEs1;
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
CREATE TEMPORARY FUNCTION unix_date AS 'it.uniroma3.kucha.Unix2Date';
CREATE TEMPORARY FUNCTION ranking AS 'it.uniroma3.kucha.Rank';

CREATE TABLE resultEs1 AS
	SELECT ps.tm, ps.productId, ps.avg FROM (
		SELECT unix_date(time) AS tm, productId, ROUND(AVG(CAST(score AS FLOAT)),3) AS avg FROM reviews GROUP BY unix_date(time), productId
	)ps
	ORDER BY ps.tm, ps.avg DESC;

SELECT tm, productId,avg from resultEs1
WHERE ranking(tm,avg) < 5;
