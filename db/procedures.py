
INSERT_AGG_OPTION_STATS = """

	INSERT INTO
		aggoptionstats{modifier} (
			date_current, 
			ticker, 
			call_volume, 
			put_volume, 
			call_open_interest,
			put_open_interest
		)
	SELECT
		date_current,
		ticker,
		call_volume,
		put_volume,
		call_open_interest,
		put_open_interest
	FROM
		(
		SELECT
			date_current,
			ticker,
			SUM(IF(option_type = "C", volume, 0)) AS call_volume,
			SUM(IF(option_type = "P", volume, 0)) AS put_volume,
			SUM(IF(option_type = "C", open_interest, 0)) AS call_open_interest,
			SUM(IF(option_type = "P", open_interest, 0)) AS put_open_interest
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
		"Ticker-Dates Map",
		"Ticker-OptionID Map",
		"Option Counts",
		"Analysis Counts",
		"Key Stats Counts"
	]

	DERIVED_PROCEDURES = [
		INSERT_AGG_OPTION_STATS.format(modifier=MODIFIER, subset="AND" + SUBSET, date=date),
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