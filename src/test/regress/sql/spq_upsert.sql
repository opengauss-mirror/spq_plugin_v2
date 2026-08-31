-- Test ON DUPLICATE KEY UPDATE on distributed tables

CREATE SCHEMA test_upsert;
SET search_path TO test_upsert;
SET spq.shard_count TO 2;

-- Basic table with primary key
CREATE TABLE upsert_basic(
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    val INT DEFAULT 0
);

SELECT create_distributed_table('upsert_basic', 'id');

-- Insert initial data
INSERT INTO upsert_basic VALUES(1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30);

SELECT * FROM upsert_basic ORDER BY id;

-- Test 1: ON DUPLICATE KEY UPDATE NOTHING (no conflict)
INSERT INTO upsert_basic VALUES(4, 'Dave', 40) ON DUPLICATE KEY UPDATE NOTHING;

SELECT * FROM upsert_basic ORDER BY id;

-- Test 2: ON DUPLICATE KEY UPDATE NOTHING (with conflict, should do nothing)
INSERT INTO upsert_basic VALUES(1, 'Alice2', 100) ON DUPLICATE KEY UPDATE NOTHING;

SELECT * FROM upsert_basic ORDER BY id;

-- Test 3: ON DUPLICATE KEY UPDATE with constant SET
INSERT INTO upsert_basic VALUES(1, 'Alice_new', 999)
    ON DUPLICATE KEY UPDATE name = 'Alice_updated', val = 99;

SELECT * FROM upsert_basic ORDER BY id;

-- Test 4: ON DUPLICATE KEY UPDATE using VALUES()
INSERT INTO upsert_basic VALUES(2, 'Bob_new', 200)
    ON DUPLICATE KEY UPDATE val = VALUES(val);

SELECT * FROM upsert_basic ORDER BY id;

-- Test 5: Multi-row upsert
INSERT INTO upsert_basic VALUES(1, 'A', 1000), (5, 'Eve', 50)
    ON DUPLICATE KEY UPDATE val = VALUES(val);

SELECT * FROM upsert_basic ORDER BY id;

-- Test 6: Expression with table self-reference
INSERT INTO upsert_basic VALUES(3, 'Charlie2', 100)
    ON DUPLICATE KEY UPDATE val = upsert_basic.val + 70;

SELECT * FROM upsert_basic ORDER BY id;

-- Test 7: Partition key modification - blocked by openGauss kernel
INSERT INTO upsert_basic VALUES(1, 'test', 0)
    ON DUPLICATE KEY UPDATE id = 100;

-- Test 8: Mutable function - blocked by SPQ planner
INSERT INTO upsert_basic VALUES(1, 'test', 0)
    ON DUPLICATE KEY UPDATE val = random() * 100;

-- Test 9: Table with composite unique constraint including dist key
CREATE TABLE upsert_composite(
    id INT,
    name TEXT,
    val INT DEFAULT 0,
    UNIQUE(id, name)
);

SELECT create_distributed_table('upsert_composite', 'id');

INSERT INTO upsert_composite VALUES(1, 'Alice', 10), (2, 'Bob', 20);

-- Upsert on composite unique
INSERT INTO upsert_composite VALUES(1, 'Alice', 99)
    ON DUPLICATE KEY UPDATE val = VALUES(val);

SELECT * FROM upsert_composite ORDER BY id, name;

-- No conflict case
INSERT INTO upsert_composite VALUES(1, 'Carol', 30)
    ON DUPLICATE KEY UPDATE val = 999;

SELECT * FROM upsert_composite ORDER BY id, name;

-- Cleanup
DROP SCHEMA test_upsert CASCADE;
RESET SEARCH_PATH;
