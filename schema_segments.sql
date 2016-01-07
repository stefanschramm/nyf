CREATE TABLE segments(
	file_id INTEGER,
	msgid VARCHAR(1024),
	bytes_nzb INTEGER,
	filename VARCHAR(1024),
	part INTEGER,
	total INTEGER,
	begin INTEGER,
	end INTEGER,
	pcrc32 INTEGER,
	tries INTEGER,
	errors INTEGER,
	complete TINYINT(1)
);
