-- one table: 10 indexed bigint, version/chk for the triggers, 168-byte payload
BEGIN;
DROP TABLE IF EXISTS t;
CREATE TABLE t (
  id bigint PRIMARY KEY,
  k0 bigint, k1 bigint, k2 bigint, k3 bigint, k4 bigint,
  k5 bigint, k6 bigint, k7 bigint, k8 bigint, k9 bigint,
  version bigint NOT NULL, chk bigint NOT NULL,
  payload text NOT NULL
) WITH (fillfactor = 85);
INSERT INTO t
SELECT g,
  (random()*9.2e18)::bigint, (random()*9.2e18)::bigint, (random()*9.2e18)::bigint, (random()*9.2e18)::bigint, (random()*9.2e18)::bigint,
  (random()*9.2e18)::bigint, (random()*9.2e18)::bigint, (random()*9.2e18)::bigint, (random()*9.2e18)::bigint, (random()*9.2e18)::bigint,
  0, 0, repeat('x', 168)
FROM generate_series(1, :rows) g;
COMMIT;
SET maintenance_work_mem = '128MB';
CREATE INDEX t_k0 ON t (k0); CREATE INDEX t_k1 ON t (k1); CREATE INDEX t_k2 ON t (k2);
CREATE INDEX t_k3 ON t (k3); CREATE INDEX t_k4 ON t (k4); CREATE INDEX t_k5 ON t (k5);
CREATE INDEX t_k6 ON t (k6); CREATE INDEX t_k7 ON t (k7); CREATE INDEX t_k8 ON t (k8);
CREATE INDEX t_k9 ON t (k9);

-- 10 identical BEFORE UPDATE triggers: version += 1; chk ^= version
CREATE OR REPLACE FUNCTION trg() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.version := NEW.version + 1;
  NEW.chk := NEW.chk # NEW.version;
  RETURN NEW;
END $$;
CREATE TRIGGER t0 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t1 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t2 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t3 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t4 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t5 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t6 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t7 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t8 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE TRIGGER t9 BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION trg();
CREATE EXTENSION IF NOT EXISTS pg_prewarm;
VACUUM (ANALYZE) t;
CHECKPOINT;
