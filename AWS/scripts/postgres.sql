CREATE TABLE cases (
  region varchar(30),
  zone varchar(30),
  instance varchar(30),
  dayOfWeek integer,
  hourOfDay integer,
  additionAllowed numeric(3,2),
  censored boolean,
  initTime bigint,
  endTime bigint,
  timeToRevocation bigint,
  skipRecords integer,
  initValue numeric(12,5),
  endValue numeric(12,5)
);
S
CREATE INDEX IF NOT EXISTS IDX_INSTANCE ON cases (instance);
CREATE INDEX IF NOT EXISTS IDX_REGION_ZONE_INSTANCE ON cases (region, zone, instance);
CREATE INDEX IF NOT EXISTS IDX_DAY_OF_WEEK ON cases (dayOfWeek);
CREATE INDEX IF NOT EXISTS IDX_HOUR_OF_DAY ON cases (hourOfDay);
CREATE INDEX IF NOT EXISTS IDX_INSTANCE_DAY_HOUR ON cases (instance, dayOfWeek, hourOfDay);
CREATE INDEX IF NOT EXISTS IDX_INIT_TIME ON cases (initTime);
CREATE INDEX IF NOT EXISTS IDX_END_TIME ON cases (endTime);


CREATE TABLE spotprice (
  id integer NOT NULL,
  region varchar(30) NOT NULL,
  zone varchar(30) NOT NULL,
  instance varchar(20) NOT NULL,
  time bigint NOT NULL,
  timestamp timestamp NOT NULL,
  price decimal(20,10) NOT NULL,
  inserted_date timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  execution_time timestamp NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS IDX_INSTANCE ON spotprice (instance);
CREATE INDEX IF NOT EXISTS IDX_ALL ON spotprice (region,zone,instance,time);
CREATE INDEX IF NOT EXISTS IDX_EXECUTION_TIME ON spotprice (execution_time);