TIMESERIES_PROC = """
DROP TABLE IF EXISTS timeseries;
CREATE TABLE timeseries (lag SMALLINT, date_current DATE);
SET @i = -1;
INSERT INTO timeseries
SELECT
	(@i:=@i+1) as lag,
	date_current
FROM
	(
		SELECT
			DISTINCT date_current
		FROM
			ohlc
	) as t1;
"""