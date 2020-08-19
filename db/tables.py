OPTIONS_TABLE = """
	CREATE TABLE options (
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
		open_interest INT UNSIGNED
		PRIMARY KEY (date_current, option_id)
	)
	PARTITION BY HASH(ticker)
	PARTITIONS 10
"""

OHLC_TABLE = """
	CREATE TABLE ohlc (
		ticker VARCHAR(10),
		date_current DATE,
		open_price FLOAT(4),
		high_price FLOAT(4),
		low_price FLOAT(4),
		close_price FLOAT(4),
		adj_close_price FLOAT(4),
		volume INT UNSIGNED,
		dividend_yield FLOAT(6)
		PRIMARY KEY (ticker, date_current)
	)
"""

KEYSTATS_TABLE = """
	CREATE TABLE keystats (
		ticker VARCHAR(10),
		date_current DATE,
		feature VARCHAR(100),
		modifier VARCHAR(100),
		value VARCHAR(100)
	)
"""

ANALYSIS_TABLE = """
	CREATE TABLE analysis (
		ticker VARCHAR(10),
		date_current DATE,
		category VARCHAR(100),
		feature VARCHAR(100),
		feature_two VARCHAR(100),
		modifier VARCHAR(100),
		value VARCHAR(100)
	)
"""

INSTRUMENT_TABLE = """
	CREATE TABLE instruments (
		last_updated DATE,
		ticker VARCHAR(10),
		name VARCHAR(100),
		exchange_code VARCHAR(100),
		exchange_name VARCHAR(50),
		sector VARCHAR(100),
		industry VARCHAR(100),
		instrument_type CHAR(10),
		market_cap BIGINT
		PRIMARY KEY(ticker, exchange_code)
	)
"""

TREASURYRATES_TABLE = """
	CREATE TABLE treasuryrates (
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
		_30_years FLOAT(4),
	)
"""

TREASURYRATEMAP_TABLE = """
	CREATE TABLE treasuryratemap (
		date_current DATE PRIMARY KEY NOT NULL,
		days_to_expiry SMALLINT UNSIGNED,
		treasuryrate FLOAT(4)
		PRIMARY KEY(date_current, days_to_expiry)
	)
"""

TICKERDATES_TABLE = """
	CREATE TABLE tickerdates (
		ticker VARCAHR(10),
		date_current DATE
		PRIMARY KEY(ticker, date_current)
	)
"""

TICKEROIDS_TABLE = """
	CREATE TABLE tickeroids (
		ticker VARCHAR(10),
		option_id VARCHAR(50),
		PRIMARY KEY(ticker, option_id)
	)
"""

###################################################################################################

POSITIONTAGS_TABLE = """
	CREATE TABLE positiontags (
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
	CREATE TABLE timeandsales (
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
	CREATE TABLE users (
		username TEXT PRIMARY KEY NOT NULL,
		password TEXT NOT NULL
	)
"""