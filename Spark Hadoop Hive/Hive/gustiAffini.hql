--import data from csv and create table;
DROP TABLE reviews;
DROP TABLE resultEs3;


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

--CREATE TABLE pairUsers AS
	--SELECT ps.user1, ps.user2, COUNT(DISTINCT(productId)) AS cont, 


CREATE TABLE resultEs3 AS
SELECT r1.userId AS user1, r2.userId AS user2, COUNT(DISTINCT(r1.productId)) AS cont, collect_set(r1.productId) as prodList 
FROM reviews r1 JOIN reviews r2 ON r1.productId = r2.productId
WHERE r1.score>3 AND r2.score>3 AND r1.userId < r2.userId
GROUP BY r1.userId, r2.userId;

SELECT user1, user2, prodList FROM resultEs3 WHERE cont>2 ORDER BY user1, user2;
 
