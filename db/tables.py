OPTIONS_TABLE = """
	CREATE TABLE optionsBACK (
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
	CREATE TABLE ohlcBACK (
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
	CREATE TABLE keystatsBACK (
		ticker VARCHAR(10),
		date_current DATE,
		feature VARCHAR(100),
		modifier VARCHAR(100),
		value VARCHAR(100)
	)
"""

ANALYSIS_TABLE = """
	CREATE TABLE analysisBACK (
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
	CREATE TABLE instrumentsBACK (
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
		_30_years FLOAT(4),
	)
"""

TREASURYRATEMAP_TABLE = """
	CREATE TABLE treasuryratemapBACK (
		date_current DATE PRIMARY KEY NOT NULL,
		days_to_expiry SMALLINT UNSIGNED,
		treasuryrate FLOAT(4)
		PRIMARY KEY(date_current, days_to_expiry)
	)
"""

TICKERDATES_TABLE = """
	CREATE TABLE tickerdatesBACK (
		ticker VARCAHR(10),
		date_current DATE
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

columns = ""
for expiry in [1, 3, 6, 9, 12, 18, 24]:
	for moneyness in range(80, 125, 5):
		columns += f"m{expiry}m{moneyness} FLOAT(6), "

SURFACE_TABLE = """
	CREATE TABLE surfaceBACK (
		ticker VARCHAR(10),
		date_current DATE,
		{columns}
		PRIMARY KEY(ticker, date_current)
	)
""".format(columns=columns)


TIMESURFACE_TABLE = """
	CREATE TABLE timesurfaceBACK (
		ticker VARCHAR(10),
		date_current DATE,
		m1 FLOAT(6),
		m3 FLOAT(6),
		m6 FLOAT(6),
		m9 FLOAT(6),
		m12 FLOAT(6),
		m18 FLOAT(6),
		m24 FLOAT(6),
		PRIMARY KEY(ticker, date_current)
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