use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

#[derive(Debug)]
struct Slot<V> {
    value: V,
    put_at_ms: u64,
}

#[derive(Debug)]
pub struct CachedMap<K, V> {
    history: HashMap<K, Slot<V>>,
    history_queue: VecDeque<(u64, K)>,
    timeout_ms: u64,
    max_size: usize,
}

impl<K, V> CachedMap<K, V>
where
    K: Eq + Hash + Copy,
{
    pub fn new(timeout_ms: u64, max_size: usize) -> Self {
        Self {
            history: HashMap::new(),
            history_queue: VecDeque::new(),
            timeout_ms,
            max_size,
        }
    }

    pub fn insert(&mut self, now_ms: u64, key: K, value: V) {
        self.history.insert(key, Slot { value, put_at_ms: now_ms });
        self.history_queue.push_back((now_ms, key));
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.history.contains_key(key)
    }

    #[allow(unused)]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.history.get(key).map(|slot| &slot.value)
    }

    #[allow(unused)]
    pub fn delete(&mut self, key: &K) {
        self.history.remove(key);
    }

    pub fn refresh(&mut self, now_ms: u64) {
        while let Some((ts, _)) = self.history_queue.front() {
            if *ts + self.timeout_ms > now_ms && self.history.len() <= self.max_size {
                break;
            }
            let (put_ms, pkt) = self.history_queue.pop_front().expect("should have pkt");
            if let Some(slot) = self.history.get(&pkt) {
                if slot.put_at_ms == put_ms {
                    self.history.remove(&pkt).expect("should have pkt");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn check_simple() {
        let mut history = super::CachedMap::new(1000, 10);
        history.insert(0, 1, 1);
        history.insert(0, 2, 2);

        assert_eq!(history.get(&1), Some(&1));
        assert_eq!(history.get(&2), Some(&2));
        assert_eq!(history.get(&3), None);

        assert_eq!(history.history.len(), 2);
        assert_eq!(history.history_queue.len(), 2);
    }

    #[test]
    fn check_timeout() {
        let mut history = super::CachedMap::new(1000, 10);
        history.insert(0, 1, 1);
        history.insert(1000, 2, 2);
        history.refresh(1001);
        assert_eq!(history.get(&1), None);
        assert_eq!(history.get(&2), Some(&2));
        assert_eq!(history.get(&3), None);

        assert_eq!(history.history.len(), 1);
        assert_eq!(history.history_queue.len(), 1);
    }

    #[test]
    fn check_insert_overwrite() {
        let mut history = super::CachedMap::new(1000, 10);
        history.insert(0, 1, 1);
        history.insert(1000, 1, 2);

        assert_eq!(history.get(&1), Some(&2));
        history.refresh(1001);
        assert_eq!(history.get(&1), Some(&2));

        history.refresh(2001);
        assert_eq!(history.get(&1), None);

        assert_eq!(history.history_queue.len(), 0);
    }
}
