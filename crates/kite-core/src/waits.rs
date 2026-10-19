//! The wait-for graph: an edge `a → b` means agent `a` sent `b` a blocking
//! request that is still open. A new edge that would close a cycle is refused,
//! so agents can never all be waiting on each other.

use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Default)]
pub struct WaitGraph {
    edges: BTreeMap<String, BTreeMap<String, u32>>,
}

impl WaitGraph {
    /// The chain `from → … → to` if one exists.
    pub fn path(&self, from: &str, to: &str) -> Option<Vec<String>> {
        if from == to {
            return Some(vec![from.to_string()]);
        }
        let mut prev: BTreeMap<&str, &str> = BTreeMap::new();
        let mut queue = VecDeque::from([from]);
        while let Some(at) = queue.pop_front() {
            for next in self.edges.get(at).into_iter().flat_map(|m| m.keys()) {
                if next == from || prev.contains_key(next.as_str()) {
                    continue;
                }
                prev.insert(next, at);
                if next == to {
                    let mut chain = vec![to.to_string()];
                    let mut cur = to;
                    while let Some(p) = prev.get(cur) {
                        chain.push(p.to_string());
                        cur = p;
                    }
                    chain.reverse();
                    return Some(chain);
                }
                queue.push_back(next);
            }
        }
        None
    }

    /// Adds `from → to` unless it would close a cycle; on refusal returns the cycle.
    pub fn try_add(&mut self, from: &str, to: &str) -> Result<(), Vec<String>> {
        if let Some(mut back) = self.path(to, from) {
            back.insert(0, from.to_string());
            return Err(back);
        }
        *self.edges.entry(from.to_string()).or_default().entry(to.to_string()).or_insert(0) += 1;
        Ok(())
    }

    pub fn remove(&mut self, from: &str, to: &str) {
        if let Some(m) = self.edges.get_mut(from) {
            if let Some(c) = m.get_mut(to) {
                *c -= 1;
                if *c == 0 {
                    m.remove(to);
                }
            }
            if m.is_empty() {
                self.edges.remove(from);
            }
        }
    }

    pub fn pairs(&self) -> Vec<(String, String)> {
        self.edges.iter().flat_map(|(a, m)| m.keys().map(move |b| (a.clone(), b.clone()))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn refuses_the_edge_that_closes_a_cycle() {
        let mut g = WaitGraph::default();
        g.try_add("a", "b").unwrap();
        g.try_add("b", "c").unwrap();
        assert_eq!(g.try_add("c", "a").unwrap_err(), vec!["c", "a", "b", "c"]);
        g.remove("a", "b");
        g.try_add("c", "a").unwrap();
    }

    #[test]
    fn parallel_edges_are_counted() {
        let mut g = WaitGraph::default();
        g.try_add("a", "b").unwrap();
        g.try_add("a", "b").unwrap();
        g.remove("a", "b");
        assert!(g.try_add("b", "a").is_err());
        g.remove("a", "b");
        assert!(g.try_add("b", "a").is_ok());
    }

    fn has_cycle(g: &WaitGraph) -> bool {
        g.pairs().iter().any(|(a, b)| g.path(b, a).is_some())
    }

    proptest! {
        #[test]
        fn graph_never_holds_a_cycle(ops in proptest::collection::vec((0u8..6, 0u8..6, any::<bool>()), 0..200)) {
            let mut g = WaitGraph::default();
            let name = |i: u8| format!("a{i}");
            for (a, b, add) in ops {
                if a == b { continue; }
                if add { let _ = g.try_add(&name(a), &name(b)); } else { g.remove(&name(a), &name(b)); }
                prop_assert!(!has_cycle(&g));
            }
        }
    }
}
