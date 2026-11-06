-- UP
CREATE TABLE collateral_watermark (
 id INTEGER PRIMARY KEY CHECK(id=1),
 number INTEGER NOT NULL CHECK(number>=0), hash TEXT NOT NULL
);
-- DOWN
DROP TABLE collateral_watermark;
