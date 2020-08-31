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

OHLCSTATS_PROC = """
	
	SELECT
		date_current,
		ticker,
		SQRT(((SUM(POWER(pct_change, 2) * _21) - (POWER(SUM(_21 * pct_change), 2) / 21)) / 20) * 252 ) * 100 AS hvol1m,
		SQRT(((SUM(POWER(pct_change, 2) * _42) - (POWER(SUM(_42 * pct_change), 2) / 42)) / 41) * 252 ) * 100 AS hvol2m,
		SQRT(((SUM(POWER(pct_change, 2) * _63) - (POWER(SUM(_63 * pct_change), 2) / 63)) / 62) * 252 ) * 100 AS hvol3m,
		SQRT(((SUM(POWER(pct_change, 2) * _126) - (POWER(SUM(_126 * pct_change), 2) / 126)) / 125) * 252 ) * 100 AS hvol6m,
		SQRT(((SUM(POWER(pct_change, 2) * _189) - (POWER(SUM(_189 * pct_change), 2) / 189)) / 188) * 252 ) * 100 AS hvol9m,
		SQRT(((SUM(POWER(pct_change, 2) * _252) - (POWER(SUM(_252 * pct_change), 2) / 252)) / 251) * 252 ) * 100 AS hvol12m,
		SUM(stock_volume * _10) / 10 AS avgvolume10,
		SUM(stock_volume * _21) / 21 AS avgvolume21,
		SUM(stock_volume * _42) / 42 AS avgvolume42,
		SUM(stock_volume * _63) / 63 AS avgvolume63,
		SUM(stock_volume * _126) / 126 AS avgvolume126,
		SUM(stock_volume * _189) / 189 AS avgvolume189,
		SUM(stock_volume * _252) / 252 AS relvolume252,
		SUM(stock_volume * _0d) / (SUM(stock_volume * _10) / 10) AS relvolume10,
		SUM(stock_volume * _0d) / (SUM(stock_volume * _21) / 21) AS relvolume21,
		SUM(stock_volume * _0d) / (SUM(stock_volume * _42) / 42) AS relvolume42,
		SUM(stock_volume * _0d) / (SUM(stock_volume * _63) / 63) AS relvolume63,
		SUM(stock_volume * _0d) / (SUM(stock_volume * _126) / 126) AS relvolume126,
		SUM(stock_volume * _0d) / (SUM(stock_volume * _189) / 189) AS relvolume189,
		SUM(stock_volume * _0d) / (SUM(stock_volume * _252) / 252) AS relvolume252,
		pct_change as pctchange1d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _5d) - 1) as pctchange5d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _10d) - 1) as pctchange10d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _21d) - 1) as pctchange21d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _42d) - 1) as pctchange42d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _63d) - 1) as pctchange63d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _126d) - 1) as pctchange126d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _189d) - 1) as pctchange189d,
		100 * (SUM(adj_close * _0d) / SUM(adj_close * _252d) - 1) as pctchange252d
	FROM
		(
			SELECT
				o1.date_current,
				o1.ticker,
				o1.stock_volume,
				o1.adj_close,
				(o1.adj_close / o2.adj_close - 1) AS pct_change,
				d1.*
			FROM
				ohlc AS o1
			INNER JOIN
				dateseries AS d1
				ON o1.date_current = d1.lag_date
				INNER JOIN
					ohlc AS o2
					ON o2.date_current = d1.prev_lag_date 
					AND o2.ticker = o1.ticker
		) as t1
	GROUP BY
		ticker;
	
"""