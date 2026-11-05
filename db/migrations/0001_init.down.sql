-- 0001_init.down.sql — reverse of 0001_init.up.sql
BEGIN;

DROP TABLE IF EXISTS envelope_receipt;
DROP TABLE IF EXISTS message_recipient;
DROP TABLE IF EXISTS message;
DROP TABLE IF EXISTS person;

COMMIT;
