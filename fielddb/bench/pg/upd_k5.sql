\set id random(1, :rows)
\set v random(1, 9223372036854775806)
UPDATE t SET k5 = :v WHERE id = :id;
