DROP SCHEMA IF EXISTS aws;
CREATE SCHEMA IF NOT EXISTS aws;

USE aws;

CREATE TABLE IF NOT EXISTS spotprice (
  id INT NOT NULL AUTO_INCREMENT,
  region VARCHAR(30) NOT NULL,
  zone VARCHAR(30) NOT NULL,
  instance VARCHAR(20) NOT NULL,
  time BIGINT(20) NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  price DECIMAL(20,10) NOT NULL,
  inserted_date TIMESTAMP NOT NULL DEFAULT NOW(),
  execution_time TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE case_old (
  instance varchar(30) DEFAULT NULL,
  dayOfWeek int(11) DEFAULT NULL,
  hourOfDay int(11) DEFAULT NULL,
  additionAllowed double DEFAULT NULL,
  censured tinyint(1) DEFAULT NULL,
  initTime bigint(20) DEFAULT NULL,
  endTime bigint(20) DEFAULT NULL,
  timeToRevocation bigint(20) DEFAULT NULL,
  skipRecords int(11) DEFAULT NULL,
  actualValue double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

CREATE TABLE case_new (
  instance varchar(30) DEFAULT NULL,
  dayOfWeek int(11) DEFAULT NULL,
  hourOfDay int(11) DEFAULT NULL,
  additionAllowed double DEFAULT NULL,
  censured tinyint(1) DEFAULT NULL,
  initTime bigint(20) DEFAULT NULL,
  endTime bigint(20) DEFAULT NULL,
  timeToRevocation bigint(20) DEFAULT NULL,
  skipRecords int(11) DEFAULT NULL,
  actualValue double DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=UTF8;

ALTER TABLE spotprice 
ADD INDEX IDX_ALL (region ASC, zone ASC, instance ASC, time ASC),
ADD UNIQUE INDEX UNIQUE_ALL (region ASC, zone ASC, instance ASC, time ASC);
