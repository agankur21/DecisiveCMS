CREATE TABLE log_data (
  log_id text,
  time timeuuid,
  ip text,
  PRIMARY KEY(log_id, time)
);
