CREATE KEYSPACE cache_manager WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
CREATE TABLE cache(
   url text,
   data text,
   extra text,
   created_at TIMESTAMP,
   updated_at TIMESTAMP,
   PRIMARY KEY (url)
);

CREATE TABLE logs(
   id UUID,
   activity text,
   ip_address text,
   user_agent text,
   user_id UUID,
   created_at TIMESTAMP,
   PRIMARY KEY (id, ip_address, user_id)
);

CREATE CUSTOM INDEX cache_data ON cache (data) USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = { 'mode': 'CONTAINS' };
