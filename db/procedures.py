INITDATESERIES = """

	DROP TABLE IF EXISTS dateseries;
	CREATE TABLE dateseries (
		lag SMALLINT,
		lag_date DATE,
		prev_lag_date DATE, 
		_5 SMALLINT,
		_10 SMALLINT,
		_20 SMALLINT,
		_21 SMALLINT,
		_42 SMALLINT,
		_63 SMALLINT,
		_126 SMALLINT,
		_189 SMALLINT,
		_252 SMALLINT,
		_0d SMALLINT,
		_5d SMALLINT,
		_10d SMALLINT,
		_20d SMALLINT,
		_21d SMALLINT,
		_42d SMALLINT,
		_63d SMALLINT,
		_126d SMALLINT,
		_189d SMALLINT,
		_252d SMALLINT
	);

	SET @i = -1;
	INSERT INTO dateseries
	SELECT
		(@i:=@i+1) AS lag,
		date_current AS lag_date,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL
	FROM
		(SELECT
			DISTINCT date_current
		FROM
			ohlc 
		WHERE
			date_current <= "2020-08-27"
		GROUP BY 
			date_current DESC) AS t1;
		
	UPDATE
		dateseries AS d1
	INNER JOIN
		dateseries AS d2
		ON d1.lag = (d2.lag - 1)
	SET
		d1.prev_lag_date = d2.lag_date,
		d1._5 = IF(d1.lag < 5, 1, 0),
		d1._10 = IF(d1.lag < 10, 1, 0),
		d1._20 = IF(d1.lag < 20, 1, 0),
		d1._21 = IF(d1.lag < 21, 1, 0),
		d1._42 = IF(d1.lag < 42, 1, 0),
		d1._63 = IF(d1.lag < 63, 1, 0),
		d1._126 = IF(d1.lag < 126, 1, 0),
		d1._189 = IF(d1.lag < 189, 1, 0),
		d1._252 = IF(d1.lag < 252, 1, 0),
		d1._0d = IF(d1.lag = 0, 1, 0),
		d1._5d = IF(d1.lag = 5, 1, 0),
		d1._10d = IF(d1.lag = 10, 1, 0),
		d1._20d = IF(d1.lag = 20, 1, 0),
		d1._21d = IF(d1.lag = 21, 1, 0),
		d1._42d = IF(d1.lag = 42, 1, 0),
		d1._63d = IF(d1.lag = 63, 1, 0),
		d1._126d = IF(d1.lag = 126, 1, 0),
		d1._189d = IF(d1.lag = 189, 1, 0),
		d1._252d = IF(d1.lag = 252, 1, 0);

"""

INITAGGOPTIONSTATS = """

	DROP TABLE IF EXISTS aggoptionstats;
	CREATE TABLE aggoptionstats (
		date_current DATE,
		ticker VARCHAR(10),
		call_volume BIGINT,
		put_volume BIGINT,
		cpv_spread BIGINT,
		total_volume BIGINT,
		rcv5 FLOAT,
		rpv5 FLOAT,
		rtv5 FLOAT,
		rcv10 FLOAT,
		rpv10 FLOAT,
		rtv10 FLOAT,
		rcv20 FLOAT,
		rpv20 FLOAT,
		rtv20 FLOAT,
		rcps5 FLOAT,
		rcps10 FLOAT,
		rcps20 FLOAT
	);

	INSERT INTO aggoptionstats
	SELECT
		date_current,
		ticker,
		call_volume,
		put_volume,
		call_volume - put_volume AS cpv_spread,
		total_volume,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL,
		NULL
	FROM
		(
		SELECT
			date_current,
			ticker,
			SUM(IF(option_type = "C", volume, 0)) as call_volume,
			SUM(IF(option_type = "P", volume, 0)) as put_volume,
			SUM(volume) as total_volume
		FROM
			options
		GROUP BY
			ticker,
			date_current
		ORDER BY
			ticker ASC,
			date_current DESC
		) as t1;

"""

OHLCSTATS = """
	
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

AGGOPTIONSTATS = """

	SELECT
	*
	FROM
		(
			SELECT
				ticker,
				date_current,
				SUM(_0d * call_volume) / (SUM(call_volume * _5) / 5) as rcv5,
				SUM(_0d * put_volume) / (SUM(put_volume * _5) / 5) as rpv5,
				SUM(_0d * total_volume) / (SUM(total_volume * _5) / 5) as rtv5,
				SUM(_0d * call_volume) / (SUM(call_volume * _10) / 10) as rcv10,
				SUM(_0d * put_volume) / (SUM(put_volume * _10) / 10) as rpv10,
				SUM(_0d * total_volume) / (SUM(total_volume * _10) / 10) as rtv10,
				SUM(_0d * call_volume) / (SUM(call_volume * _20) / 20) as rcv20,
				SUM(_0d * put_volume) / (SUM(put_volume * _20) / 20) as rpv20,
				SUM(_0d * total_volume) / (SUM(total_volume * _20) / 20) as rtv20,
				SUM(_0d * call_volume) / (SUM(call_volume * _20) / 20) as rcv20,
				SUM(_0d * put_volume) / (SUM(put_volume * _20) / 20) as rpv20,
				SUM(_0d * total_volume) / (SUM(total_volume * _20) / 20) as rtv20,
				SUM(_0d * total_volume) / (SUM(total_volume * _20) / 20) as rtv20,
				SUM(_0d * total_volume) / (SUM(total_volume * _20) / 20) as rtv20,
				SUM(_0d * cpv_spread) / (SUM(cpv_spread * _5) / 5) as rcpvs5,
				SUM(_0d * cpv_spread) / (SUM(cpv_spread * _10) / 10) as rcpvs10,
				SUM(_0d * cpv_spread) / (SUM(cpv_spread * _20) / 20) as rcpvs20
			FROM
				aggoptionstats AS o
			INNER JOIN
				dateseries d
				ON o.date_current = d.lag_date
			GROUP BY
				ticker
		) as t1
	WHERE
		date_current = "2020-08-27";

"""

OPTIONSTATS = """

	SELECT
		date_current,
		option_id,
		100 * ((SUM(_0d * option_price) / SUM(_1d * option_price)) - 1) as pctchange1d,
		100 * ((SUM(_0d * option_price) / SUM(_5d * option_price)) - 1) as pctchange5d,
		100 * ((SUM(_0d * option_price) / SUM(_10d * option_price)) - 1) as pctchange10d,
		100 * ((SUM(_0d * option_price) / SUM(_20d * option_price)) - 1) as pctchange20d,
		100 * (SUM(_0d * implied_volatility) - SUM(_1d * implied_volatility)) as ivchange1d,
		100 * (SUM(_0d * implied_volatility) - SUM(_5d * implied_volatility)) as ivchange5d,
		100 * (SUM(_0d * implied_volatility) - SUM(_10d * implied_volatility)) as ivchange10d,
		100 * (SUM(_0d * implied_volatility) - SUM(_20d * implied_volatility)) as ivchange20d,
		SUM(_0d * volume) / (SUM(_5 * volume) / 5) as relvolume5,
		SUM(_0d * volume) / (SUM(_10 * volume) / 10) as relvolume10,
		SUM(_0d * volume) / (SUM(_20 * volume) / 20) as relvolume20
	FROM
		options AS o
	INNER JOIN
		(
			SELECT
				*
			FROM
				dateseries
			WHERE
				lag < 20
		) AS d
		ON
			o.date_current = d.lag_date
	WHERE
		ticker in ("AAPL", "TSLA")
	GROUP BY
		option_id
	ORDER BY
		date_current DESC,
		option_id ASC;

"""