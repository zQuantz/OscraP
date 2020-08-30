DATESERIES_PROC = """

DROP TABLE IF EXISTS dateseries;
CREATE TABLE dateseries (lag SMALLINT, lag_date DATE, prev_lag_date DATE);

SET @i = -1;
INSERT INTO dateseries
SELECT
	(@i:=@i+1) AS lag,
	date_current AS lag_date,
	NULL
FROM
	(SELECT
		DISTINCT date_current
	FROM
		ohlc 
	GROUP BY 
		date_current DESC) AS t1;
	
UPDATE
	dateseries AS d1
INNER JOIN
	dateseries AS d2
	ON d1.lag = (d2.lag - 1)
SET
	d1.prev_lag_date = d2.lag_date;

"""

HVOL_PROC = """

SET @lag_date = (
	SELECT
		lag_date
	FROM
		dateseries
	WHERE
		lag = {LAG}
);

SELECT
	*
FROM
	(
		SELECT
			o1.date_current,
			o1.ticker,
			STD((o1.adj_close / o2.adj_close ) - 1) * SQRT(252) * 100
		FROM
			ohlc AS o1
		INNER JOIN
			dateseries AS d1
			ON o1.date_current = d1.lag_date
			INNER JOIN
				ohlc AS o2
				ON o2.date_current = d1.prev_lag_date 
				AND o2.ticker = o1.ticker
		WHERE
			o1.date_current >= @lag_date
		GROUP BY
			ticker
	) as t1
WHERE
	t1.date_current = {DATE};

"""