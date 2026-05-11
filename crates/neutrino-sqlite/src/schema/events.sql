CREATE TABLE events (
    stream_ordering INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    state_key TEXT,
    event_json TEXT NOT NULL
);

CREATE UNIQUE INDEX events_event_id_idx ON events(event_id);
