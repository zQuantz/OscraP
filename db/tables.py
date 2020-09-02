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
		volume INT UNSIGNED,
		open_interest INT UNSIGNED,
		PRIMARY KEY (date_current, option_id)
	)
"""

OPTIONSTATS_TABLE = """
	CREATE TABLE optionstatsBACK (
		date_current DATE,
		option_id VARCHAR(40),
		pctchange1d FLOAT(4),
		pctchange5d FLOAT(4),
		pctchange10d FLOAT(4),
		pctchange20d FLOAT(4),
		ivchange1d FLOAT(4),
		ivchange5d FLOAT(4),
		ivchange10d FLOAT(4),
		ivchange20d FLOAT(4),
		relvolume5 FLOAT(4),
		relvolume10 FLOAT(4),
		relvolume20 FLOAT(4),
		PRIMARY KEY(date_current, option_id)
	)
"""

AGGOPTIONSTATS_TABLE = """
	CREATE TABLE aggoptionstatsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		call_volume BIGINT,
		put_volume BIGINT,
		cpv_spread BIGINT,
		total_volume BIGINT,
		rcv5 FLOAT(4),
		rpv5 FLOAT(4),
		rtv5 FLOAT(4),
		rcv10 FLOAT(4),
		rpv10 FLOAT(4),
		rtv10 FLOAT(4),
		rcv20 FLOAT(4),
		rpv20 FLOAT(4),
		rtv20 FLOAT(4),
		rcpvs5 FLOAT(4),
		rcpvs10 FLOAT(4),
		rcpvs20 FLOAT(4),
		PRIMARY KEY(date_current, ticker)
	)
"""

OHLC_TABLE = """
	CREATE TABLE ohlcBACK (
		date_current DATE,
		ticker VARCHAR(10),
		open_price FLOAT(4),
		high_price FLOAT(4),
		low_price FLOAT(4),
		close_price FLOAT(4),
		adjclose_price FLOAT(4),
		volume INT UNSIGNED,
		dividend_yield FLOAT(6),
		PRIMARY KEY(date_current, ticker)
	)
"""

OHLCSTATS_TABLE = """
	CREATE TABLE ohlcstatsBACK (
		date_current DATE,
		ticker VARCHAR(10),
		hvol1m FLOAT(4),
		hvol2m FLOAT(4),
		hvol3m FLOAT(4),
		hvol6m FLOAT(4),
		hvol9m FLOAT(4),
		hvol12m FLOAT(4),
		avgvolume10 BIGINT,
		avgvolume21 BIGINT,
		avgvolume42 BIGINT,
		avgvolume63 BIGINT,
		avgvolume126 BIGINT,
		avgvolume189 BIGINT,
		avgvolume252 BIGINT,
		relvolume10 FLOAT(4),
		relvolume21 FLOAT(4),
		relvolume42 FLOAT(4),
		relvolume63 FLOAT(4),
		relvolume126 FLOAT(4),
		relvolume189 FLOAT(4),
		relvolume252 FLOAT(4),
		pctchange1d FLOAT(4),
		pctchange5d FLOAT(4),
		pctchange10d FLOAT(4),
		pctchange21d FLOAT(4),
		pctchange42d FLOAT(4),
		pctchange63d FLOAT(4),
		PRIMARY KEY(date_current, ticker)
	)
"""

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

columns = ""
for expiry in [1, 3, 6, 9, 12, 18, 24]:
	for moneyness in range(80, 125, 5):
		columns += f"m{expiry}m{moneyness} FLOAT(6), "

SURFACE_TABLE = """
	CREATE TABLE surfaceBACK (
		date_current DATE,
		ticker VARCHAR(10),
		{columns}
		PRIMARY KEY(ticker, date_current)
	)
""".format(columns=columns)

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