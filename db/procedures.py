from const import EXPIRATIONS, MONEYNESSES, PCTILES, DELTAS

###################################################################################################

INIT_DATE_SERIES = """
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
			date_current < "{date}"
		ORDER BY 
			date_current DESC) t1;
"""

UPDATE_DATE_SERIES = """
	UPDATE
		dateseries AS d1
	INNER JOIN
		dateseries AS d2
		ON d1.lag = (d2.lag - 1)
	SET
		d1.prev_lag_date = d2.lag_date,
		d1._5 = IF(d1.lag < 5, 1, NULL),
		d1._10 = IF(d1.lag < 10, 1, NULL),
		d1._20 = IF(d1.lag < 20, 1, NULL),
		d1._21 = IF(d1.lag < 21, 1, NULL),
		d1._42 = IF(d1.lag < 42, 1, NULL),
		d1._63 = IF(d1.lag < 63, 1, NULL),
		d1._126 = IF(d1.lag < 126, 1, NULL),
		d1._189 = IF(d1.lag < 189, 1, NULL),
		d1._252 = IF(d1.lag < 252, 1, NULL),
		d1._0d = IF(d1.lag = 0, 1, NULL),
		d1._1d = IF(d1.lag = 1, 1, NULL),
		d1._5d = IF(d1.lag = 5, 1, NULL),
		d1._10d = IF(d1.lag = 10, 1, NULL),
		d1._20d = IF(d1.lag = 20, 1, NULL),
		d1._21d = IF(d1.lag = 21, 1, NULL),
		d1._42d = IF(d1.lag = 42, 1, NULL),
		d1._63d = IF(d1.lag = 63, 1, NULL),
		d1._126d = IF(d1.lag = 126, 1, NULL),
		d1._189d = IF(d1.lag = 189, 1, NULL),
		d1._252d = IF(d1.lag = 252, 1, NULL);
"""

###################################################################################################

INSERT_OHLC_STATS = """

	INSERT INTO
		ohlcstats{modifier}
	SELECT
		*
	FROM
		(
			SELECT
				MAX(date_current) AS date_current,
				ticker,
				SUM(volume * _0d) / AVG(volume * _10) AS relvolume10,
				SUM(volume * _0d) / AVG(volume * _21) AS relvolume21,
				SUM(volume * _0d) / AVG(volume * _42) AS relvolume42,
				SUM(volume * _0d) / AVG(volume * _63) AS relvolume63,
				SUM(volume * _0d) / AVG(volume * _126) AS relvolume126,
				SUM(volume * _0d) / AVG(volume * _189) AS relvolume189,
				SUM(volume * _0d) / AVG(volume * _252) AS relvolume252,
				100 * t1.pct_change AS pctchange1d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _5d) - 1) AS pctchange5d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _10d) - 1) AS pctchange10d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _21d) - 1) AS pctchange21d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _42d) - 1) AS pctchange42d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _63d) - 1) AS pctchange63d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _126d) - 1) AS pctchange126d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _189d) - 1) AS pctchange189d,
				100 * (SUM(adjclose_price * _0d) / SUM(adjclose_price * _252d) - 1) AS pctchange252d
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
		date_current = "{date}"
"""

columns, names = "", ""
for expiry in EXPIRATIONS:
	columns += f"STDDEV(pct_change * _{expiry * 21}) * SQRT(252) * 100 AS rvol{expiry}m,\n"
	names += f"rvol{expiry}m, \n"

INSERT_OHLC_RVOL = """
	INSERT INTO
		ohlcrvol{modifier} """ + f"""
		(
			{names}
		)
	SELECT
		*
	FROM
		(
			SELECT
				MAX(date_current) AS date_current,
				ticker,
				{columns}
			FROM
				(
					SELECT
						o1.date_current,
						o1.ticker,
						o1.volume,
						o1.adjclose_price,
						(o1.adjclose_price / o2.adjclose_price - 1) AS pct_change,
						d1.*
					FROM """ + """
						ohlc{modifier} as o1
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
		date_current = "{date}"
"""

columns, names = "", []
for expiry in EXPIRATIONS:
	for pctile in PCTILES:
		
		fn = f"rvol{expiry}m"
		name = f"{fn}p{pctile}"
		l = f"_{pctile}"

		columns += f"( (SUM({fn} * _0d) - MIN({fn} * {l})) / (MAX({fn} * {l}) - MIN({fn} * {l})) ) AS {name}"
		names.append("ohlcrvol{modifier}." + f"{name} = t2.{name}, ")

UPDATE_OHLC_RVOL = """
	UPDATE
		ohlcrvol{modifier} """ + f"""
	INNER JOIN
		(
			SELECT
				*
			FROM
				(
					SELECT
						MAX(date_current) as date_current,
						ticker,
						{columns} """ + """
					FROM
						ohlcrvol{modifier} AS o
					INNER JOIN
						dateseries d
						ON o.date_current = d.lag_date
					{subset}
					GROUP BY
						ticker
				) AS t1
			WHERE
				date_current = "{date}"
		) as t2
	USING
		(ticker, date_current)
		""" + f"""
	SET
		{"".join(names)}
"""

###################################################################################################

INSERT_AGG_OPTION_STATS = """

	INSERT INTO
		aggoptionstats{modifier} (
			date_current, 
			ticker, 
			call_volume, 
			put_volume, 
			cpv_ratio, 
			total_volume,
			call_open_interest,
			put_open_interest,
			total_open_interest,
			call_v2oi,
			put_v2oi,
			total_v2oi
		)
	SELECT
		date_current,
		ticker,
		call_volume,
		put_volume,
		call_volume / put_volume AS cpv_ratio,
		total_volume,
		call_open_interest,
		put_open_interest,
		total_open_interest,
		call_volume / call_open_interest AS call_v2oi,
		put_volume / put_open_interest AS put_v2oi,
		total_volume / total_open_interest AS total_v2oi
	FROM
		(
		SELECT
			date_current,
			ticker,
			SUM(IF(option_type = "C", volume, 0)) AS call_volume,
			SUM(IF(option_type = "P", volume, 0)) AS put_volume,
			SUM(volume) AS total_volume,
			SUM(IF(option_type = "C", open_interest, 0)) AS call_open_interest,
			SUM(IF(option_type = "P", open_interest, 0)) AS put_open_interest,
			SUM(open_interest) AS total_open_interest
		FROM
			options{modifier}
		WHERE
			date_current = "{date}"
		{subset}
		GROUP BY
			ticker,
			date_current
		ORDER BY
			ticker ASC,
			date_current DESC
		) AS t1;

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
						MAX(date_current) as date_current,
						ticker,
						SUM(_0d * call_volume) / AVG(call_volume * _5) AS rcv5,
						SUM(_0d * put_volume) / AVG(put_volume * _5) AS rpv5,
						SUM(_0d * total_volume) / AVG(total_volume * _5) AS rtv5,
						SUM(_0d * call_volume) / AVG(call_volume * _10) AS rcv10,
						SUM(_0d * put_volume) / AVG(put_volume * _10) AS rpv10,
						SUM(_0d * total_volume) / AVG(total_volume * _10) AS rtv10,
						SUM(_0d * call_volume) / AVG(call_volume * _20) AS rcv20,
						SUM(_0d * put_volume) / AVG(put_volume * _20) AS rpv20,
						SUM(_0d * total_volume) / AVG(total_volume * _20) AS rtv20,
						SUM(_0d * cpv_ratio) / AVG(cpv_ratio * _5) AS rcpvs5,
						SUM(_0d * cpv_ratio) / AVG(cpv_ratio * _10) AS rcpvs10,
						SUM(_0d * cpv_ratio) / AVG(cpv_ratio * _20) AS rcpvs20,
						SUM(_0d * call_v2oi) / AVG(call_v2oi * _5) AS rcv2oi5,
						SUM(_0d * call_v2oi) / AVG(call_v2oi * _10) AS rcv2oi10,
						SUM(_0d * call_v2oi) / AVG(call_v2oi * _20) AS rcv2oi20,
						SUM(_0d * put_v2oi) / AVG(put_v2oi * _5) AS rpv2oi5,
						SUM(_0d * put_v2oi) / AVG(put_v2oi * _10) AS rpv2oi10,
						SUM(_0d * put_v2oi) / AVG(put_v2oi * _20) AS rpv2oi20,
						SUM(_0d * total_v2oi) / AVG(total_v2oi * _5) AS rtv2oi5,
						SUM(_0d * total_v2oi) / AVG(total_v2oi * _10) AS rtv2oi10,
						SUM(_0d * total_v2oi) / AVG(total_v2oi * _20) AS rtv2oi20
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
				date_current = "{date}"
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
		aggoptionstats{modifier}.rcpvs20 = t2.rcpvs20,
		aggoptionstats{modifier}.rcv2oi5 = t2.rcv2oi5,
		aggoptionstats{modifier}.rcv2oi10 = t2.rcv2oi10,
		aggoptionstats{modifier}.rcv2oi20 = t2.rcv2oi20,
		aggoptionstats{modifier}.rpv2oi5 = t2.rpv2oi5,
		aggoptionstats{modifier}.rpv2oi10 = t2.rpv2oi10,
		aggoptionstats{modifier}.rpv2oi20 = t2.rpv2oi20,
		aggoptionstats{modifier}.rtv2oi5 = t2.rtv2oi5,
		aggoptionstats{modifier}.rtv2oi10 = t2.rtv2oi10,
		aggoptionstats{modifier}.rtv2oi20 = t2.rtv2oi20;

"""

INSERT_OPTION_STATS = """

	INSERT INTO
		optionstats{modifier}
	SELECT
		*
	FROM
		(
			SELECT
				MAX(date_current) as date_current,
				ticker,
				option_id,
				100 * ((SUM(_0d * option_price) / SUM(_1d * option_price)) - 1) AS pctchange1d,
				100 * ((SUM(_0d * option_price) / SUM(_5d * option_price)) - 1) AS pctchange5d,
				100 * ((SUM(_0d * option_price) / SUM(_10d * option_price)) - 1) AS pctchange10d,
				100 * ((SUM(_0d * option_price) / SUM(_20d * option_price)) - 1) AS pctchange20d,
				100 * ((SUM(_0d * (0.5 * (bid_price + ask_price) )) / SUM(_1d * (0.5 * (bid_price + ask_price) ))) - 1) AS midpctchange1d,
				100 * ((SUM(_0d * (0.5 * (bid_price + ask_price) )) / SUM(_5d * (0.5 * (bid_price + ask_price) ))) - 1) AS midpctchange5d,
				100 * ((SUM(_0d * (0.5 * (bid_price + ask_price) )) / SUM(_10d * (0.5 * (bid_price + ask_price) ))) - 1) AS midpctchange10d,
				100 * ((SUM(_0d * (0.5 * (bid_price + ask_price) )) / SUM(_20d * (0.5 * (bid_price + ask_price) ))) - 1) AS midpctchange20d,
				(SUM(_0d * zimplied_volatility) - SUM(_1d * zimplied_volatility)) AS ivchange1d,
				(SUM(_0d * zimplied_volatility) - SUM(_5d * zimplied_volatility)) AS ivchange5d,
				(SUM(_0d * zimplied_volatility) - SUM(_10d * zimplied_volatility)) AS ivchange10d,
				(SUM(_0d * zimplied_volatility) - SUM(_20d * zimplied_volatility)) AS ivchange20d,
				SUM(_0d * volume) / AVG(_5 * volume) AS relvolume5,
				SUM(_0d * volume) / AVG(_10 * volume) AS relvolume10,
				SUM(_0d * volume) / AVG(_20 * volume) AS relvolume20,
				SUM(_0d * (volume / open_interest)) / AVG(_5 * (volume / open_interest)) AS relvolume2oi5,
				SUM(_0d * (volume / open_interest)) / AVG(_10 * (volume / open_interest)) AS relvolume2oi10,
				SUM(_0d * (volume / open_interest)) / AVG(_20 * (volume / open_interest)) AS relvolume2oi20
			FROM
				options{modifier} AS o
			INNER JOIN
				(
					SELECT
						*
					FROM
						dateseries
					WHERE
						lag <= 20
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
		date_current = "{date}"

"""

###################################################################################################

skews = {
	"f" : [90, 110],
	"d" : [90, 100],
	"u" : [100, 110]
}

columns = ""
for expiry in EXPIRATIONS:
	for skew in skews:
		s1, s2 = skews[skew]
		columns += f"m{expiry}{s1} - m{expiry}{s2} AS m{expiry}{skew},"

INSERT_SURFACE_SKEW = """
	INSERT INTO
		surfaceskew{modifier}
	SELECT
		date_current,
		ticker, """ + f"""
		{columns}
	FROM """ + """
		surface{modifier}
	WHERE
		date_current = "{date}"
	{subset}
"""

columns, names = "", ""
for expiry in EXPIRATIONS:

	for pctile in PCTILES:

		fn = f"m{expiry}m100"
		l = f"_{pctile}"
		name = f"{fn}p{pctile}"

		columns += f"( (SUM({fn} * _0d) - MIN({fn} * {l})) / (MAX({fn} * {l}) - MIN({fn} * {l})) ) AS {name}"
		names += f"{name},"

	for delta in DELTAS:
	
		fn = f"m{expiry}m100"
		l = f"_{pctile}d"
		name = f"{fn}change{delta}d"

		columns += f"SUM(_0d * {fn}) - SUM({l} * {fn} AS {name}"
		names += f"{name},"

INSERT_SURFACE_STATS = """

	INSERT INTO
		surfacestats{modifier} ( """ + f"""
			{names}
		)
	SELECT
		*
	FROM
		(
			SELECT
				MAX(date_current) as date_current,
				ticker,
				{columns} """ + """
			FROM
				surface{modifier} AS o
			INNER JOIN
				dateseries d
				ON o.date_current = d.lag_date
			{subset}
			GROUP BY
				ticker
			ORDER BY
				date_current DESC
		) as t1
	WHERE
		date_current = "{date}"

"""

columns, names = "", ""
for expiry in EXPIRATIONS:
	for skew in skews:
		for pctile in PCTILES:

			fn = f"m{expiry}{skew}"
			l = f"_{pctile}"
			name = f"{fn}p{pctile}"

			columns += f"( (SUM({fn} * _0d) - MIN({fn} * {l})) / (MAX({fn} * {l}) - MIN({fn} * {l})) ) AS {name}, "
			names += f"{name},"


INSERT_SURFACE_SKEW_PCTILE = """

	INSERT INTO
		surfaceskewpctile{modifier} ( """ + f"""
			{names}
		)
	SELECT
		*
	FROM
		(
			SELECT
				MAX(date_current) as date_current,
				ticker,
				{columns} """ + """
			FROM
				surfaceskew{modifier} AS o
			INNER JOIN
				dateseries d
				ON o.date_current = d.lag_date
			{subset}
			GROUP BY
				ticker
			ORDER BY
				date_current DESC
		) as t1
	WHERE
		date_current = "{date}"

"""

###################################################################################################

expirations = [1,3,6,12,18,24]
moneys = list(range(80, 125, 5))
lags = ["_63", "_126", "_252"]
lag_names = ["3", "6", "12"]

first_ops = ""
for e in expirations:
	for m in moneys:
		for lag, lag_name in zip(lags, lag_names):
			first_ops += f"m{e}m{m} * {lag} AS m{e}m{m}w{lag_name}, \n"
first_ops = first_ops[:-3]

second_ops = ""
for lag, lag_name in zip(lags, lag_names):
	for e in expirations:
		for m in moneys:
			label = f"m{e}m{m}w{lag_name}"
			second_ops += f"MIN({label}) AS {label}min, \n"
			second_ops += f"MAX({label}) AS {label}max, \n"
			second_ops += f"AVG({label}) AS {label}mean, \n"
			second_ops += f"100 * (SUM({label} * _0d) - MIN({label})) / (MAX({label}) - MIN({label})) AS {label}rank, \n"
			second_ops += f"(SUM({label} * _0d) - AVG({label})) / STDDEV({label}) AS {label}zscore, \n"
second_ops = second_ops[:-3]

INSERT_SURFACE_STATS = ("""
		INSERT INTO
			surfacestats{modifier}
		SELECT
			*
		FROM
			(
				SELECT
		""" + f"""
					MAX(date_current) as date_current,
					ticker,
					{second_ops}
				FROM
					(
						SELECT
							date_current,
							ticker,
							_0d,
							{first_ops}
			""" + """		
						FROM
							surface{modifier}
						INNER JOIN
							dateseries
							on lag_date = date_current
						{subset}
					) as t1
				GROUP BY
					ticker
				ORDER BY
					ticker ASC,
					date_current DESC
			) as t2
		WHERE
			date_current = "{date}";
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
		date_current = "{date}"
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
		date_current = "{date}"
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
		date_current = "{date}"
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
		date_current = "{date}"
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
		date_current = "{date}"
	{subset}
	GROUP BY
		date_current,
		ticker;

"""

###################################################################################################

SPLIT_OHLC_UPDATE = """
	UPDATE
		ohlc{modifier}
	SET
		open_price = ROUND(open_price * {factor}, 2),
		high_price = ROUND(high_price * {factor}, 2),
		low_price = ROUND(low_price * {factor}, 2),
		close_price = ROUND(close_price * {factor}, 2),
		adjclose_price = ROUND(adjclose_price * {factor}, 2),
		volume = ROUND(volume / {factor}, 0)
	WHERE
		date_current
		BETWEEN "{d1}" AND "{d2}"
	AND date_current < "{d2}"
	AND ticker = "{ticker}";
"""

SPLIT_AGG_UPDATE = """
	UPDATE
		aggoptionstats{modifier}
	SET
		call_volume = ROUND(call_volume / {factor}, 0),
		put_volume = ROUND(put_volume / {factor}, 0),
		total_volume = ROUND(total_volume / {factor}, 0),
		call_open_interest = ROUND(call_open_interest / {factor}, 0),
		put_open_interest = ROUND(put_open_interest / {factor}, 0),
		total_open_interest = ROUND(total_open_interest / {factor}, 0)
	WHERE
		date_current
		BETWEEN "{d1}" AND "{d2}"
	AND date_current < "{d2}"
	AND ticker = "{ticker}";
"""

SPLIT_OPTIONS_UPDATE = """
	UPDATE
		options{modifier}
	SET
		bid_price = ROUND(bid_price * {factor}, 2),
		ask_price = ROUND(ask_price * {factor}, 2),
		option_price = ROUND(option_price * {factor}, 2),
		strike_price = ROUND(strike_price * {factor}, 2),
		volume = ROUND(volume / {factor}, 0),
		open_interest = ROUND(open_interest / {factor}, 0)
	WHERE
		date_current
		BETWEEN "{d1}" AND "{d2}"
	AND date_current < "{d2}"
	AND ticker = "{ticker}";
"""

SPLIT_TICKEROIDS_UPDATE = """
	INSERT IGNORE INTO
		tickeroids{modifier}
	SELECT
		ticker,
		option_id
	FROM
		options{modifier}
	WHERE
		date_current
		BETWEEN "{d1}" AND "{d2}"
	AND ticker = "{ticker}"
	GROUP BY
		ticker,
		option_id
"""

SPLIT_OPTION_ID_UPDATE = """
	UPDATE
		options{modifier} AS o,
		optionstats{modifier} AS os
	SET
		o.option_id = CONCAT(o.ticker, " ", o.expiration_date, " ", o.option_type, o.strike_price),
		os.option_id = CONCAT(o.ticker, " ", o.expiration_date, " ", o.option_type, o.strike_price)
	WHERE
		o.date_current = os.date_current
	AND o.option_id = os.option_id
	AND o.date_current
		BETWEEN "{d1}" AND "{d2}"
	AND o.date_current < "{d2}"
	AND o.ticker = "{ticker}";
"""

UPDATE_SPLIT_STATUS = """
	UPDATE
		stocksplitstatus{modifier}
	SET
		processed_timestamp = CURRENT_TIMESTAMP()
	WHERE
		ticker = "{ticker}"
	AND d1 = "{d1}"
	AND d2 = "{d2}"
	AND ex_date = "{ex_date}"
	AND procedure_name = "{procedure_name}";
"""

SPLIT_PROCEDURES = {
	"SPLIT_OHLC_UPDATE" : SPLIT_OHLC_UPDATE,
	"SPLIT_AGG_UPDATE" : SPLIT_AGG_UPDATE,
	"SPLIT_OPTIONS_UPDATE" : SPLIT_OPTIONS_UPDATE,
	"SPLIT_OPTION_ID_UPDATE" : SPLIT_OPTION_ID_UPDATE,
	"SPLIT_TICKEROIDS_UPDATE" : SPLIT_TICKEROIDS_UPDATE
}

###################################################################################################

def get_derived_procedures(date):

	MODIFIER = ""
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
		INSERT_AGG_OPTION_STATS.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date),
		INSERT_OHLC_STATS.format(modifier=MODIFIER,
								 subset="WHERE" + SUBSET.replace("ticker IN", "o1.ticker IN"),
								 date=date),
		UPDATE_AGG_OPTION_STATS.format(modifier=MODIFIER, subset="WHERE" + SUBSET, date=date),
		INSERT_OPTION_STATS.format(modifier=MODIFIER, subset="WHERE" + SUBSET, date=date),
		INSERT_SURFACE_STATS.format(modifier=MODIFIER, subset="WHERE" + SUBSET, date=date),
		INSERT_SURFACE_SKEW.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date),
		INSERT_TICKER_DATES.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date),
		INSERT_TICKER_OIDS.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date),
		INSERT_OPTION_COUNTS.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date),
		INSERT_ANALYSIS_COUNTS.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date),
		INSERT_KEYSTATS_COUNTS.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date)
	]

	return {
		name : procedure
		for name, procedure
		in zip(DERIVED_PROCEDURE_NAMES, DERIVED_PROCEDURES)
	}