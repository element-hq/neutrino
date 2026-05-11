use rusqlite::Connection;
use serde_json::Value;

const SCHEMA: &str = include_str!("schema/events.sql");

#[derive(Debug)]
pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open_in_memory() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        Self { conn }
    }

    pub fn insert_events(&mut self, events: &[Value]) {
        let tx = self.conn.transaction().unwrap();
        let mut stmt = tx
            .prepare("INSERT INTO events (event_id, room_id, event_type, state_key, event_json) VALUES (?, ?, ?, ?, ?)")
            .unwrap();

        for event in events {
            let event_id = event.pointer("/event_id").unwrap().as_str().unwrap();
            let room_id = event.pointer("/room_id").unwrap().as_str().unwrap();
            let event_type = event.pointer("/type").unwrap().as_str().unwrap();
            let state_key = event
                .pointer("/state_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let event_json = serde_json::to_string(event).unwrap();

            stmt.execute(rusqlite::params![
                event_id, room_id, event_type, state_key, event_json
            ])
            .unwrap();
        }

        std::mem::drop(stmt);

        tx.commit().unwrap();
    }

    pub fn count_distinct_rooms(&self) -> i64 {
        self.conn
            .query_one("SELECT COUNT(DISTINCT room_id) FROM events", [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    pub fn events_after(&self, pos: i64) -> Vec<(i64, String, Value)> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT stream_ordering, room_id, event_json FROM events \
                 WHERE stream_ordering > ? ORDER BY stream_ordering ASC",
            )
            .unwrap();
        let mut rows = stmt.query([pos]).unwrap();

        let mut out = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let stream_ordering: i64 = row.get(0).unwrap();
            let room_id: String = row.get(1).unwrap();
            let event_json: String = row.get(2).unwrap();
            let event: Value = serde_json::from_str(&event_json).unwrap();
            out.push((stream_ordering, room_id, event));
        }
        out
    }

    pub fn members_of(&self, room_id: &str) -> Vec<Value> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT event_json FROM events \
                 WHERE room_id = ? AND event_type = 'm.room.member'",
            )
            .unwrap();
        let mut rows = stmt.query([room_id]).unwrap();

        let mut members = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let event_json: String = row.get(0).unwrap();
            members.push(serde_json::from_str(&event_json).unwrap());
        }
        members
    }
}
