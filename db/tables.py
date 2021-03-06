
EXPIRATIONS = [1,2,3,6,9,12,18,24]
MONEYNESSES = list(range(80, 125, 5))

###################################################################################################

OPTIONS_TABLE = """
	CREATE TABLE optionsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		expiration_date DATE,
		days_to_expiry SMALLINT UNSIGNED,
		option_id VARCHAR(40),
		option_type CHAR(1),
		strike_price FLOAT(4),
		bid_price FLOAT(4),
		option_price FLOAT(4),
		ask_price FLOAT(4),
		implied_volatility FLOAT(6),
		zimplied_volatility FLOAT(6),
		volume INT UNSIGNED,
		open_interest INT UNSIGNED,
		INDEX(date_current, option_id)
	)
"""

AGGOPTIONSTATS_TABLE = """
	CREATE TABLE aggoptionstatsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		call_volume BIGINT,
		put_volume BIGINT,
		call_open_interest BIGINT,
		put_open_interest BIGINT,
		PRIMARY KEY(date_current, ticker)
	)
"""

###################################################################################################

OHLC_TABLE = """
	CREATE TABLE ohlcBACK (
		date_current DATE,
		ticker VARCHAR(10),
		open_price FLOAT(4),
		high_price FLOAT(4),
		low_price FLOAT(4),
		close_price FLOAT(4),
		adjclose_price FLOAT(4),
		volume BIGINT UNSIGNED,
		dividend_yield FLOAT(6),
		PRIMARY KEY(date_current, ticker)
	)
"""

###################################################################################################

columns = []
for expiry in EXPIRATIONS:
	for moneyness in MONEYNESSES:
		columns.append(f"m{expiry}m{moneyness} FLOAT(4), \n")

SURFACE_TABLE = """
	CREATE TABLE surfaceBACK (
		date_current DATE,
		ticker VARCHAR(10),
		{columns}
		PRIMARY KEY(ticker, date_current)
	)
""".format(columns="".join(columns))

###################################################################################################

KEYSTATS_TABLE = """
	CREATE TABLE keystatsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		feature VARCHAR(100),
		modifier VARCHAR(100),
		value VARCHAR(100)
	)
"""

ANALYSIS_TABLE = """
	CREATE TABLE analysisBACK (
		date_current DATE,
		ticker VARCHAR(10),
		category VARCHAR(100),
		feature VARCHAR(100),
		feature_two VARCHAR(100),
		modifier VARCHAR(100),
		value VARCHAR(100)
	)
"""

INSTRUMENT_TABLE = """
	CREATE TABLE instrumentsBACK (
		last_updated DATE,
		ticker VARCHAR(10),
		name VARCHAR(100),
		exchange_code VARCHAR(10),
		exchange_name VARCHAR(50),
		sector VARCHAR(100),
		industry VARCHAR(100),
		instrument_type CHAR(10),
		market_cap BIGINT,
		PRIMARY KEY(ticker, exchange_code)
	)
"""

TREASURYRATES_TABLE = """
	CREATE TABLE treasuryratesBACK (
		date_current DATE PRIMARY KEY NOT NULL,
		_1_month FLOAT(4),
		_2_months FLOAT(4),
		_3_months FLOAT(4),
		_6_months FLOAT(4),
		_1_year FLOAT(4),
		_2_years FLOAT(4),
		_3_years FLOAT(4),
		_5_years FLOAT(4),
		_7_years FLOAT(4),
		_10_years FLOAT(4),
		_20_years FLOAT(4),
		_30_years FLOAT(4)
	)
"""

TREASURYRATEMAP_TABLE = """
	CREATE TABLE treasuryratemapBACK (
		date_current DATE,
		days_to_expiry SMALLINT UNSIGNED,
		rate FLOAT(6),
		PRIMARY KEY(date_current, days_to_expiry)
	)
"""

###################################################################################################

BATCHTICKERS_TABLE = """
	CREATE TABLE batchtickers (
		ticker VARCHAR(10) PRIMARY KEY NOT NULL
	)
"""

TICKERDATES_TABLE = """
	CREATE TABLE tickerdatesBACK (
		ticker VARCHAR(10),
		date_current DATE,
		PRIMARY KEY(ticker, date_current)
	)
"""

TICKEROIDS_TABLE = """
	CREATE TABLE tickeroidsBACK (
		ticker VARCHAR(10),
		option_id VARCHAR(50),
		PRIMARY KEY(ticker, option_id)
	)
"""

OPTIONCOUNTS_TABLE = """
	CREATE TABLE optionscountsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		count MEDIUMINT UNSIGNED,
		PRIMARY KEY(date_current, ticker) 
	)
"""

ANALYSISCOUNTS_TABLE = """
	CREATE TABLE analysiscountsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		count MEDIUMINT UNSIGNED,
		PRIMARY KEY(date_current, ticker) 
	)
"""

KEYSTATSCOUNTS_TABLE = """
	CREATE TABLE keystatscountsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		count MEDIUMINT UNSIGNED,
		PRIMARY KEY(date_current, ticker) 
	)
"""

STOCKSPLITS_TABLE = """
	CREATE TABLE stocksplitsBACK (
		ticker VARCHAR(10),
		split_factor FLOAT(4),
		announcement_date DATE,
		record_date DATE,
		ex_date DATE,
		processed_timestamp TIMESTAMP NULL DEFAULT NULL,
		UNIQUE KEY(ticker, ex_date)
	)
"""
STOCKSPLITSTMP_TABLE = STOCKSPLITS_TABLE.replace("stocksplits", "stocksplitstmp")

STOCKSPLITSTATUS_TABLE = """
	CREATE TABLE stocksplitstatusBACK (
		ticker VARCHAR(10),
		ex_date DATE,
		procedure_order TINYINT,
		procedure_name CHAR(25),
		d1 DATE,
		d2 DATE,
		split_factor FLOAT(4),
		processed_timestamp TIMESTAMP NULL DEFAULT NULL,
		UNIQUE KEY(ticker, ex_date, procedure_name, d1, d2)
	)
"""

###################################################################################################

POSITIONTAGS_TABLE = """
	CREATE TABLE positiontagsBACK (
		username VARCHAR(30),
		execution_time TIMESTAMP,
		ticker VARCHAR(10),
		position_id VARCHAR(32),
		direction CHAR(5),
		strategy CHAR(20),
		sentiment CHAR(7),
		notes TEXT,
		image_filenames TEXT,
		images LONGTEXT
	)
"""

TIMEANDSALES_TABLE = """
	CREATE TABLE timeandsalesBACK (
		username VARCHAR(30),
		execution_time TIMESTAMP,
		position_id VARCHAR(32),
		ticker VARCHAR(10),
		date_current DATE,
		expiration_date DATE,
		days_to_expiry SMALLINT UNSIGNED,
		option_id VARCHAR(40),
		option_type CHAR(1),
		strike_price FLOAT(4),
		bid_price FLOAT(4),
		option_price FLOAT(4),
		ask_price FLOAT(4),
		implied_volatility FLOAT(6),
		volume INT UNSIGNED,
		open_interest INT UNSIGNED,
		stock_price FLOAT(4),
	)
"""

USERS_TABLE = """
	CREATE TABLE usersBACK (
		username TEXT PRIMARY KEY NOT NULL,
		password TEXT NOT NULL
	)
"""