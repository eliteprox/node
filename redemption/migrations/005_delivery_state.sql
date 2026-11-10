-- UP
ALTER TABLE winning_tickets ADD COLUMN ticket_hash BLOB;
CREATE INDEX winning_tickets_hash ON winning_tickets(ticket_hash);
CREATE TABLE ticket_state (
 hash BLOB PRIMARY KEY CHECK(length(hash)=32), sig BLOB NOT NULL,
 exposed INTEGER NOT NULL DEFAULT 0,
 settled INTEGER NOT NULL DEFAULT 0, settlement TEXT NOT NULL DEFAULT '',
 delivery TEXT NOT NULL DEFAULT 'queued',
 failures INTEGER NOT NULL DEFAULT 0, next_attempt INTEGER NOT NULL DEFAULT 0, visited INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX ticket_state_signature ON ticket_state(sig);
CREATE INDEX ticket_state_pending ON ticket_state(settled,next_attempt);
CREATE TABLE redemption_binding (
 id INTEGER PRIMARY KEY CHECK(id=1), chain_id TEXT NOT NULL,
 controller TEXT NOT NULL, role TEXT NOT NULL, destination TEXT NOT NULL
);
-- DOWN
CREATE TEMP TABLE cannot_downgrade_redemption_delivery_state (preserved INTEGER);
CREATE TEMP TRIGGER protect_delivery_state BEFORE INSERT ON cannot_downgrade_redemption_delivery_state WHEN NEW.preserved=0
 BEGIN SELECT RAISE(ABORT,'cannot_downgrade_redemption_delivery_state: retain the upgraded database'); END;
INSERT INTO cannot_downgrade_redemption_delivery_state SELECT CASE WHEN EXISTS(SELECT 1 FROM redemption_binding) OR EXISTS(SELECT 1 FROM ticket_state WHERE delivery!='queued') THEN 0 ELSE 1 END;
DROP TABLE cannot_downgrade_redemption_delivery_state;
DROP TABLE redemption_binding;
DROP TABLE ticket_state;
DROP INDEX winning_tickets_hash;
ALTER TABLE winning_tickets DROP COLUMN ticket_hash;
