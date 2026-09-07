CREATE TABLE accounts (
    id          uuid PRIMARY KEY,
    kind        text        NOT NULL CHECK (kind IN ('user', 'treasury')),
    owner       text        NOT NULL,
    currency    char(3)     NOT NULL,
    balance     bigint      NOT NULL DEFAULT 0,
    version     bigint      NOT NULL DEFAULT 1,
    created_at  timestamptz NOT NULL,
    -- Last line of defence: only the treasury may go negative.
    CONSTRAINT accounts_balance_non_negative CHECK (balance >= 0 OR kind = 'treasury')
);

-- Exactly one treasury per currency; makes EnsureTreasury race-safe.
CREATE UNIQUE INDEX accounts_treasury_per_currency
    ON accounts (currency) WHERE kind = 'treasury';

CREATE TABLE transfers (
    id               uuid PRIMARY KEY,
    from_account_id  uuid        NOT NULL REFERENCES accounts (id),
    to_account_id    uuid        NOT NULL REFERENCES accounts (id),
    amount           bigint      NOT NULL CHECK (amount > 0),
    currency         char(3)     NOT NULL,
    reference        text        NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL,
    CONSTRAINT transfers_distinct_accounts CHECK (from_account_id <> to_account_id)
);

CREATE TABLE ledger_entries (
    id             uuid PRIMARY KEY,
    transfer_id    uuid        NOT NULL REFERENCES transfers (id),
    account_id     uuid        NOT NULL REFERENCES accounts (id),
    amount         bigint      NOT NULL CHECK (amount <> 0),
    balance_after  bigint      NOT NULL,
    created_at     timestamptz NOT NULL
);

-- Keyset pagination index: (account, created_at DESC, id DESC).
CREATE INDEX ledger_entries_account_keyset
    ON ledger_entries (account_id, created_at DESC, id DESC);

CREATE TABLE outbox (
    id               bigserial PRIMARY KEY,
    event_id         uuid        NOT NULL UNIQUE,
    event_type       text        NOT NULL,
    aggregate_id     uuid        NOT NULL,
    payload          jsonb       NOT NULL,
    status           text        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'dead')),
    attempts         int         NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    sent_at          timestamptz
);

-- Partial index keeps the relay's poll cheap no matter how big the sent backlog grows.
CREATE INDEX outbox_pending_due
    ON outbox (next_attempt_at, id) WHERE status = 'pending';

CREATE TABLE idempotency_keys (
    scope            text        NOT NULL,
    key              text        NOT NULL,
    fingerprint      text        NOT NULL,
    status           text        NOT NULL CHECK (status IN ('in_progress', 'completed')),
    response_status  int,
    response_headers jsonb,
    response_body    bytea,
    created_at       timestamptz NOT NULL DEFAULT now(),
    expires_at       timestamptz NOT NULL,
    PRIMARY KEY (scope, key)
);

CREATE INDEX idempotency_keys_expiry ON idempotency_keys (expires_at);
