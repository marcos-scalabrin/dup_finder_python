-- dup_finder.delete_file definition

-- Drop table

-- DROP TABLE dup_finder.delete_file;

CREATE TABLE dup_finder.delete_file (
	id int4 NULL,
	file_name text NULL,
	file_path text NULL,
	file_size int8 NULL,
	file_extension text NULL,
	ts_atime timestamp NULL,
	ts_mtime timestamp NULL,
	ts_ctime timestamp NULL,
	uuid_hash uuid NULL,
	ts_created timestamp NULL,
	ts_run timestamp NULL,
	ts_deleted timestamp(0) NOT NULL DEFAULT now(),
	CONSTRAINT delete_file_un UNIQUE (id)
);


-- dup_finder.directory definition

-- Drop table

-- DROP TABLE dup_finder.directory;

CREATE TABLE dup_finder.directory (
	file_path text NULL,
	total_bytes numeric NULL,
	files_count int8 NULL,
	uuid_hash uuid NULL,
	ts_created timestamp(0) NOT NULL DEFAULT now(),
	id serial NOT NULL,
	CONSTRAINT directory_pk PRIMARY KEY (id),
	CONSTRAINT directory_un UNIQUE (file_path)
);


-- dup_finder.file definition

-- Drop table

-- DROP TABLE dup_finder.file;

CREATE TABLE dup_finder.file (
	id serial NOT NULL,
	file_name text NOT NULL,
	file_path text NOT NULL,
	file_size int8 NOT NULL,
	file_extension text NULL,
	ts_atime timestamp NULL,
	ts_mtime timestamp NULL,
	ts_ctime timestamp NULL,
	uuid_hash uuid NOT NULL,
	ts_created timestamp NOT NULL DEFAULT now(),
	ts_run timestamp NOT NULL,
	CONSTRAINT files_pkey PRIMARY KEY (id)
);
CREATE INDEX file_extension ON dup_finder.file USING btree (file_extension);
CREATE INDEX file_path ON dup_finder.file USING btree (file_path);
CREATE INDEX file_ts_run_idx ON dup_finder.file USING btree (ts_run);
CREATE INDEX file_uuid_hash ON dup_finder.file USING btree (uuid_hash);