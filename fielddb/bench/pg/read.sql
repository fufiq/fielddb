\set id random(1, :rows)
SELECT * FROM t WHERE id = :id;
