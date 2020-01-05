CREATE TABLE IF NOT EXISTS immigration (  	
  	cicid INTEGER PRIMARY KEY,
	i94yr INTEGER,
	i94mon INTEGER,  	
	i94cit VARCHAR	NOT NULL,
  	i94res VARCHAR,
	i94port VARCHAR,
	arrdate VARCHAR,  	
  	i94mode INTEGER
  	i94addr VARCHAR	NOT NULL,  	  	
  	depdate VARCHAR,
	i94bir INTEGER, 
	i94visa INTEGER,
	endtedpa VARCHAR,
	endtedpd VARCHAR, 
	matflag VARCHAR,	
  	airline VARCHAR,
  	fltno VARCHAR,  	
  	visatype VARCHAR,
  	gender VARCHAR	
	
);

CREATE TABLE IF NOT EXISTS world_temperature (
	id	INTEGER IDENTITY(0,1) PRIMARY KEY,
	city VARCHAR,
	country VARCHAR	NOT NULL,	
	latitude VARCHAR,
	longitude VARCHAR
	
);

CREATE TABLE IF NOT EXISTS usdemographic_info (
	id	INTEGER IDENTITY(0,1) PRIMARY KEY,
	city	VARCHAR,
	state 	VARCHAR	NOT NULL,
	medianage INTEGER,
	femalepopulation INTEGER,
	malepopulation INTEGER,
	totalpopulation INTEGER,
	foreignBorn INTEGER,
	averagehouseholdsize FLOAT,
	statecode	VARCHAR,
	race	VARCHAR,
	countofrace	VARCHAR		
);


