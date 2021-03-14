drop table dup_finder.file;


CREATE TABLE dup_finder.file (
	id serial NOT NULL,
	file_name text NOT NULL,
	file_path text NOT NULL,
	file_size int8 NOT NULL,
	file_extension text,
	ts_atime timestamp,
	ts_mtime timestamp,
	ts_ctime timestamp,
	uuid_hash uuid NOT NULL,
	ts_created timestamp Not NULL DEFAULT now(),
	ts_run timestamp NOT NULL,
	CONSTRAINT files_pkey PRIMARY KEY (id)
);
CREATE INDEX file_uuid_hash ON dup_finder.file USING btree (uuid_hash);
create index file_extension on dup_finder.file using btree (file_extension);
create index file_path on dup_finder.file using btree (file_path);

