CREATE TABLE accounts
(
    id      TEXT           NOT NULL PRIMARY KEY,
    balance NUMERIC(18, 2) NOT NULL
);

CREATE TABLE transfer_log
(
    id             BIGSERIAL      NOT NULL PRIMARY KEY,
    transaction_id TEXT           NOT NULL,
    sender_id      TEXT           NOT NULL,
    receiver_id    TEXT           NOT NULL,
    amount         NUMERIC(18, 2) NOT NULL,
    status         INTEGER        NOT NULL,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX ON transfer_log (transaction_id);