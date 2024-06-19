CREATE KEYSPACE cache_manager WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
CREATE TABLE cache(
   url text,
   data text,
   tag text,
   extra text,
   created_at TIMESTAMP,
   updated_at TIMESTAMP,
   PRIMARY KEY (url,tag)
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
CREATE CUSTOM INDEX cache_tag ON cache (tag) USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = { 'mode': 'CONTAINS' };
`



CREATE TABLE activity_logs (
    id uuid PRIMARY KEY,
    event_type text,
    type text,
    activity text,
    ip_address text,
    agent text,
    extra text,
    operator int,
    created_at timestamp,
    updated_at timestamp
);


CREATE INDEX ON activity_logs (event_type);
CREATE INDEX ON activity_logs (type);
CREATE INDEX ON activity_logs (operator);
CREATE INDEX ON activity_logs (created_at);