INIT_DATE_SERIES = [
	"DELETE FROM dateseries;",
	"SET @i = -1;",
	"""
		INSERT INTO
			dateseries (
				lag, 
				lag_date
			)
		SELECT
			(@i:=@i+1) AS lag,
			date_current AS lag_date
		FROM
			(SELECT
				DISTINCT date_current
			FROM
				ohlc{modifier}
			WHERE
				date_current <= @date_current
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
			d1._1d = IF(d1.lag = 1, 1, 0),
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
]

INSERT_AGG_OPTION_STATS = """

	INSERT INTO
		aggoptionstats{modifier} (
			date_current, 
			ticker, 
			call_volume, 
			put_volume, 
			cpv_spread, 
			total_volume
		)
	SELECT
		date_current,
		ticker,
		call_volume,
		put_volume,
		call_volume - put_volume AS cpv_spread,
		total_volume
	FROM
		(
		SELECT
			date_current,
			ticker,
			SUM(IF(option_type = "C", volume, 0)) AS call_volume,
			SUM(IF(option_type = "P", volume, 0)) AS put_volume,
			SUM(volume) AS total_volume
		FROM
			options{modifier}
		WHERE
			date_current = @date_current
		{subset}
		GROUP BY
			ticker,
			date_current
		ORDER BY
			ticker ASC,
			date_current DESC
		) AS t1;

"""

INSERT_OHLC_STATS = """

	INSERT INTO
		ohlcstats{modifier}
	SELECT
		*
	FROM
		(
			SELECT
				date_current,
				ticker,
				SQRT(((SUM(POWER(pct_change, 2) * _21) - (POWER(SUM(_21 * pct_change), 2) / 21)) / 20) * 252 ) * 100 AS hvol1m,
				SQRT(((SUM(POWER(pct_change, 2) * _42) - (POWER(SUM(_42 * pct_change), 2) / 42)) / 41) * 252 ) * 100 AS hvol2m,
				SQRT(((SUM(POWER(pct_change, 2) * _63) - (POWER(SUM(_63 * pct_change), 2) / 63)) / 62) * 252 ) * 100 AS hvol3m,
				SQRT(((SUM(POWER(pct_change, 2) * _126) - (POWER(SUM(_126 * pct_change), 2) / 126)) / 125) * 252 ) * 100 AS hvol6m,
				SQRT(((SUM(POWER(pct_change, 2) * _189) - (POWER(SUM(_189 * pct_change), 2) / 189)) / 188) * 252 ) * 100 AS hvol9m,
				SQRT(((SUM(POWER(pct_change, 2) * _252) - (POWER(SUM(_252 * pct_change), 2) / 252)) / 251) * 252 ) * 100 AS hvol12m,
				SUM(volume * _10) / 10 AS avgvolume10,
				SUM(volume * _21) / 21 AS avgvolume21,
				SUM(volume * _42) / 42 AS avgvolume42,
				SUM(volume * _63) / 63 AS avgvolume63,
				SUM(volume * _126) / 126 AS avgvolume126,
				SUM(volume * _189) / 189 AS avgvolume189,
				SUM(volume * _252) / 252 AS avgvolume252,
				SUM(volume * _0d) / (SUM(volume * _10) / 10) AS relvolume10,
				SUM(volume * _0d) / (SUM(volume * _21) / 21) AS relvolume21,
				SUM(volume * _0d) / (SUM(volume * _42) / 42) AS relvolume42,
				SUM(volume * _0d) / (SUM(volume * _63) / 63) AS relvolume63,
				SUM(volume * _0d) / (SUM(volume * _126) / 126) AS relvolume126,
				SUM(volume * _0d) / (SUM(volume * _189) / 189) AS relvolume189,
				SUM(volume * _0d) / (SUM(volume * _252) / 252) AS relvolume252,
				pct_change AS pctchange1d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _5d) - 1) AS pctchange5d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _10d) - 1) AS pctchange10d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _21d) - 1) AS pctchange21d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _42d) - 1) AS pctchange42d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _63d) - 1) AS pctchange63d
			FROM
				(
					SELECT
						o1.date_current,
						o1.ticker,
						o1.volume,
						o1.adjclose_price,
						(o1.adjclose_price / o2.adjclose_price - 1) AS pct_change,
						d1.*
					FROM
						ohlc{modifier} AS o1
					INNER JOIN
						dateseries AS d1
						ON o1.date_current = d1.lag_date
						INNER JOIN
							ohlc{modifier} AS o2
							ON o2.date_current = d1.prev_lag_date 
							AND o2.ticker = o1.ticker
					{subset}
				) AS t1
			GROUP BY
				ticker
			ORDER BY
				date_current DESC
		) as t2
	WHERE
		date_current = @date_current
	
"""

UPDATE_AGG_OPTION_STATS = """

	UPDATE
		aggoptionstats{modifier}
	INNER JOIN
		(
			SELECT
				*
			FROM
				(
					SELECT
						ticker,
						date_current,
						SUM(_0d * call_volume) / (SUM(call_volume * _5) / 5) AS rcv5,
						SUM(_0d * put_volume) / (SUM(put_volume * _5) / 5) AS rpv5,
						SUM(_0d * total_volume) / (SUM(total_volume * _5) / 5) AS rtv5,
						SUM(_0d * call_volume) / (SUM(call_volume * _10) / 10) AS rcv10,
						SUM(_0d * put_volume) / (SUM(put_volume * _10) / 10) AS rpv10,
						SUM(_0d * total_volume) / (SUM(total_volume * _10) / 10) AS rtv10,
						SUM(_0d * call_volume) / (SUM(call_volume * _20) / 20) AS rcv20,
						SUM(_0d * put_volume) / (SUM(put_volume * _20) / 20) AS rpv20,
						SUM(_0d * total_volume) / (SUM(total_volume * _20) / 20) AS rtv20,
						SUM(_0d * cpv_spread) / (SUM(cpv_spread * _5) / 5) AS rcpvs5,
						SUM(_0d * cpv_spread) / (SUM(cpv_spread * _10) / 10) AS rcpvs10,
						SUM(_0d * cpv_spread) / (SUM(cpv_spread * _20) / 20) AS rcpvs20
					FROM
						aggoptionstats{modifier} AS o
					INNER JOIN
						dateseries d
						ON o.date_current = d.lag_date
					{subset}
					GROUP BY
						ticker
				) AS t1
			WHERE
				date_current = @date_current
		) as t2
	USING
		(ticker, date_current)
	SET
		aggoptionstats{modifier}.rcv5 = t2.rcv5,
		aggoptionstats{modifier}.rpv5 = t2.rpv5,
		aggoptionstats{modifier}.rtv5 = t2.rtv5,
		aggoptionstats{modifier}.rcv10 = t2.rcv10,
		aggoptionstats{modifier}.rpv10 = t2.rpv10,
		aggoptionstats{modifier}.rtv10 = t2.rtv10,
		aggoptionstats{modifier}.rcv20 = t2.rcv20,
		aggoptionstats{modifier}.rpv20 = t2.rpv20,
		aggoptionstats{modifier}.rtv20 = t2.rtv20,
		aggoptionstats{modifier}.rcpvs5 = t2.rcpvs5,
		aggoptionstats{modifier}.rcpvs10 = t2.rcpvs10,
		aggoptionstats{modifier}.rcpvs20 = t2.rcpvs20;

"""

INSERT_OPTION_STATS = """

	INSERT INTO
		optionstats{modifier}
	SELECT
		*
	FROM
		(
			SELECT
				date_current,
				option_id,
				100 * ((SUM(_0d * option_price) / SUM(_1d * option_price)) - 1) AS pctchange1d,
				100 * ((SUM(_0d * option_price) / SUM(_5d * option_price)) - 1) AS pctchange5d,
				100 * ((SUM(_0d * option_price) / SUM(_10d * option_price)) - 1) AS pctchange10d,
				100 * ((SUM(_0d * option_price) / SUM(_20d * option_price)) - 1) AS pctchange20d,
				100 * (SUM(_0d * implied_volatility) - SUM(_1d * implied_volatility)) AS ivchange1d,
				100 * (SUM(_0d * implied_volatility) - SUM(_5d * implied_volatility)) AS ivchange5d,
				100 * (SUM(_0d * implied_volatility) - SUM(_10d * implied_volatility)) AS ivchange10d,
				100 * (SUM(_0d * implied_volatility) - SUM(_20d * implied_volatility)) AS ivchange20d,
				SUM(_0d * volume) / (SUM(_5 * volume) / 5) AS relvolume5,
				SUM(_0d * volume) / (SUM(_10 * volume) / 10) AS relvolume10,
				SUM(_0d * volume) / (SUM(_20 * volume) / 20) AS relvolume20
			FROM
				options{modifier} AS o
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
			{subset}
			GROUP BY
				option_id
			ORDER BY
				date_current DESC,
				option_id ASC
		) as t1
	WHERE
		date_current = @date_current

"""

INSERT_SURFACE_SKEW = """
	INSERT INTO
		surfaceskew{modifier}
	SELECT
		date_current,
		ticker,
		m1m90 - m1m110 as m1fskew,
		m1m90 - m1m100 as m1dskew,
		m1m100 - m1m110 as m1uskew,
		m3m90 - m3m110 as m3fskew,
		m3m90 - m3m100 as m3dskew,
		m3m100 - m3m110 as m3uskew,
		m6m90 - m6m110 as m6fskew,
		m6m90 - m6m100 as m6dskew,
		m6m100 - m6m110 as m6uskew,
		m9m90 - m9m110 as m9fskew,
		m9m90 - m9m100 as m9dskew,
		m9m100 - m9m110 as m9uskew,
		m12m90 - m12m110 as m12fskew,
		m12m90 - m12m100 as m12dskew,
		m12m100 - m12m110 as m12uskew,
		m18m90 - m18m110 as m18fskew,
		m18m90 - m18m100 as m18dskew,
		m18m100 - m18m110 as m18uskew,
		m24m90 - m24m110 as m24fskew,
		m24m90 - m24m100 as m24dskew,
		m24m100 - m24m110 as m24uskew
	FROM
		surface{modifier}
	WHERE
		date_current = @date_current
	{subset}
"""

###################################################################################################

expirations = [1,3,6,12,18,24]
moneys = list(range(80, 125, 5))
lags = ["_63", "_126", "_252"]
lag_names = ["3", "6", "12"]

ops = ""
for e in expirations:
	for m in moneys:
		ops += f"IF(m{e}m{m} * LAG_SERIES = 0, NULL, m{e}m{m} * LAG_SERIES) AS m{e}m{m}wLAG_NAME, \n"

first_ops = ""
for lag, lag_name in zip(lags, lag_names):
	_ops = ops.replace("LAG_SERIES", lag)
	_ops = _ops.replace("LAG_NAME", lag_name)
	first_ops += _ops
first_ops = first_ops[:-3]

second_ops = ""
for lag, lag_name in zip(lags, lag_names):
	for e in expirations:
		for m in moneys:
			label = f"m{e}m{m}w{lag_name}"
			second_ops += f"MIN({label}) AS {label}min, \n"
			second_ops += f"MAX({label}) AS {label}max, \n"
			second_ops += f"AVG({label}) AS {label}mean, \n"
			second_ops += f"100 * ({label} - MIN({label})) / (MAX({label}) - MIN({label})) AS {label}rank, \n"
			second_ops += f"({label} - AVG({label})) / STDDEV({label}) AS {label}zscore, \n"
second_ops = second_ops[:-3]

INSERT_SURFACE_STATS = ("""
		INSERT INTO
			surfacestats{modifier}
		SELECT
	""" + f"""
			date_current,
			ticker,
			{second_ops}
		FROM
			(
				SELECT
					date_current,
					ticker,
					{first_ops}
	""" + """
				FROM
					surface{modifier}
				INNER JOIN
					dateseries
					on lag_date = date_current
				{subset}
			) as t1
		WHERE
			@date_current = date_current;
""")

###################################################################################################


INSERT_TICKER_DATES = """

	INSERT INTO
		tickerdates{modifier}
	SELECT
		ticker,
		date_current
	FROM
		options{modifier}
	WHERE
		date_current = @date_current
	{subset}
	GROUP BY
		ticker, date_current
	ORDER BY
		ticker ASC,
		date_current DESC;

"""

INSERT_TICKER_OIDS = """

	INSERT IGNORE INTO
		tickeroids{modifier}
	SELECT
		ticker,
		option_id
	FROM
		options{modifier}
	WHERE
		date_current = @date_current
	{subset}
	GROUP BY
		ticker,
		option_id;

"""

INSERT_OPTION_COUNTS = """
	
	INSERT INTO
		optionscounts{modifier}
	SELECT
		date_current,
		ticker,
		COUNT(ticker)
	FROM
		options{modifier}
	WHERE
		date_current = @date_current
	{subset}
	GROUP BY
		date_current,
		ticker;

"""

INSERT_ANALYSIS_COUNTS = """
	
	INSERT INTO
		analysiscounts{modifier}
	SELECT
		date_current,
		ticker,
		COUNT(ticker)
	FROM
		analysis{modifier}
	WHERE
		date_current = @date_current
	{subset}
	GROUP BY
		date_current,
		ticker;

"""

INSERT_KEYSTATS_COUNTS = """
	
	INSERT INTO
		keystatscounts{modifier}
	SELECT
		date_current,
		ticker,
		COUNT(ticker)
	FROM
		keystats{modifier}
	WHERE
		date_current = @date_current
	{subset}
	GROUP BY
		date_current,
		ticker;

"""

###################################################################################################

MODIFIER = "BACK"
SUBSET = """
	 ticker IN (
		SELECT
			ticker
		FROM
			batchtickers
	)
"""

DERIVED_PROCEDURE_NAMES = [
	"Agg. Option Stats",
	"OHLC Stats",
	"Agg. Option Stats Update",
	"Option Stats",
	"Surface Stats",
	"Surface Skew",
	"Ticker-Dates Map",
	"Ticker-OptionID Map",
	"Option Counts",
	"Analysis Counts",
	"Key Stats Counts"
]

DERIVED_PROCEDURES = [
	INSERT_AGG_OPTION_STATS.format(modifier=MODIFIER, subset="AND" + SUBSET),
	INSERT_OHLC_STATS.format(modifier=MODIFIER, subset="WHERE" + SUBSET.replace("ticker IN", "o1.ticker IN")),
	UPDATE_AGG_OPTION_STATS.format(modifier=MODIFIER, subset="WHERE" + SUBSET),
	INSERT_OPTION_STATS.format(modifier=MODIFIER, subset="WHERE" + SUBSET),
	INSERT_SURFACE_STATS.format(modifier=MODIFIER, subset="WHERE" + SUBSET),
	INSERT_SURFACE_SKEW.format(modifier=MODIFIER, subset="AND" + SUBSET),
	INSERT_TICKER_DATES.format(modifier=MODIFIER, subset="AND" + SUBSET),
	INSERT_TICKER_OIDS.format(modifier=MODIFIER, subset="AND" + SUBSET),
	INSERT_OPTION_COUNTS.format(modifier=MODIFIER, subset="AND" + SUBSET),
	INSERT_ANALYSIS_COUNTS.format(modifier=MODIFIER, subset="AND" + SUBSET),
	INSERT_KEYSTATS_COUNTS.format(modifier=MODIFIER, subset="AND" + SUBSET)
]

DERIVED_PROCEDURES = {
	name : procedure
	for name, procedure
	in zip(DERIVED_PROCEDURE_NAMES, DERIVED_PROCEDURES)
}