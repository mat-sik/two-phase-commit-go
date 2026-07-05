-- +goose Up
CREATE TABLE transaction_states
(
    transaction_id TEXT    NOT NULL,
    participant_id TEXT    NOT NULL,
    state          INTEGER NOT NULL,
    PRIMARY KEY (transaction_id, participant_id)
);