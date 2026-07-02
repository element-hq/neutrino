use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// Thread-safe map for pending request/response pairs keyed by arbitrary
/// `Eq + Hash` values (device IDs, `(device_id, char_uuid)` tuples, etc.).
///
/// Useful for platform backends where responses arrive on separate callbacks
/// (CoreBluetooth delegate methods, bluer notification streams, Android JNI).
pub struct KeyedRequestMap<K: Eq + Hash, V> {
    inner: Mutex<HashMap<K, V>>,
}

impl<K: Eq + Hash, V> KeyedRequestMap<K, V> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Insert `value` at `key`, returning the previous value if any.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self.inner.lock().insert(key, value)
    }

    /// Remove and return the value at `key`, if present.
    pub fn take(&self, key: &K) -> Option<V> {
        self.inner.lock().remove(key)
    }

    /// Drain all entries — useful for cleanup on disconnect.
    pub fn drain(&self) -> Vec<(K, V)> {
        self.inner.lock().drain().collect()
    }
}

impl<K: Eq + Hash, V> Default for KeyedRequestMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe map for pending request/response pairs, keyed by
/// auto-generated sequential `u64` IDs. Built on [`KeyedRequestMap`].
pub struct RequestMap<V> {
    inner: KeyedRequestMap<u64, V>,
    next_id: AtomicU64,
}

impl<V> RequestMap<V> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: KeyedRequestMap::new(),
            next_id: AtomicU64::new(0),
        }
    }

    /// Insert `value` and return the assigned request ID.
    pub fn insert(&self, value: V) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.insert(id, value);
        id
    }

    /// Remove and return the value associated with `id`, if present.
    pub fn take(&self, id: u64) -> Option<V> {
        self.inner.take(&id)
    }
}

impl<V> Default for RequestMap<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_take() {
        let map = RequestMap::<String>::new();
        let id = map.insert("hello".to_string());
        assert_eq!(map.take(id), Some("hello".to_string()));
        assert_eq!(map.take(id), None);
    }

    #[test]
    fn keyed_insert_and_take() {
        let map = KeyedRequestMap::<String, u32>::new();
        assert_eq!(map.insert("a".into(), 1), None);
        assert_eq!(map.take(&"a".into()), Some(1));
        assert_eq!(map.take(&"a".into()), None);
    }

    use proptest::collection::vec;
    use proptest::prelude::*;

    #[derive(Debug, Clone)]
    enum RequestMapOp {
        Insert(u32),
        TakeExisting(usize),
        TakeBogus(u64),
    }

    fn request_map_op() -> impl Strategy<Value = RequestMapOp> {
        prop_oneof![
            any::<u32>().prop_map(RequestMapOp::Insert),
            any::<usize>().prop_map(RequestMapOp::TakeExisting),
            any::<u64>().prop_map(RequestMapOp::TakeBogus),
        ]
    }

    proptest! {
        /// `RequestMap::take(id)` returns `Some(v)` iff `id` was returned by a
        /// prior `insert(v)` that has not yet been taken.
        #[test]
        fn request_map_matches_hashmap_model(ops in vec(request_map_op(), 0..200)) {
            let map = RequestMap::<u32>::new();
            let mut model: HashMap<u64, u32> = HashMap::new();
            let mut issued_ids: Vec<u64> = Vec::new();

            for op in ops {
                match op {
                    RequestMapOp::Insert(v) => {
                        let id = map.insert(v);
                        prop_assert!(model.insert(id, v).is_none(), "id reuse: {id}");
                        issued_ids.push(id);
                    }
                    RequestMapOp::TakeExisting(i) if !issued_ids.is_empty() => {
                        let id = issued_ids[i % issued_ids.len()];
                        prop_assert_eq!(map.take(id), model.remove(&id));
                    }
                    RequestMapOp::TakeExisting(_) => {}
                    RequestMapOp::TakeBogus(id) => {
                        prop_assert_eq!(map.take(id), model.get(&id).copied());
                        model.remove(&id);
                    }
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    enum KeyedOp {
        Insert(u8, u32),
        Take(u8),
        Drain,
    }

    fn keyed_op() -> impl Strategy<Value = KeyedOp> {
        prop_oneof![
            (any::<u8>(), any::<u32>()).prop_map(|(k, v)| KeyedOp::Insert(k, v)),
            any::<u8>().prop_map(KeyedOp::Take),
            Just(KeyedOp::Drain),
        ]
    }

    proptest! {
        /// `KeyedRequestMap` operations behave identically to a reference
        /// `HashMap` across any mixed sequence of insert/take/drain.
        #[test]
        fn keyed_request_map_matches_hashmap_model(ops in vec(keyed_op(), 0..200)) {
            let map = KeyedRequestMap::<u8, u32>::new();
            let mut model: HashMap<u8, u32> = HashMap::new();

            for op in ops {
                match op {
                    KeyedOp::Insert(k, v) => {
                        prop_assert_eq!(map.insert(k, v), model.insert(k, v));
                    }
                    KeyedOp::Take(k) => {
                        prop_assert_eq!(map.take(&k), model.remove(&k));
                    }
                    KeyedOp::Drain => {
                        let mut drained = map.drain();
                        drained.sort_by_key(|(k, _)| *k);
                        let mut expected: Vec<_> = model.drain().collect();
                        expected.sort_by_key(|(k, _)| *k);
                        prop_assert_eq!(drained, expected);
                    }
                }
            }
        }
    }
}
