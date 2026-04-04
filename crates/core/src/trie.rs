use std::collections::HashMap;

use smallvec::SmallVec;

/// Unique identifier for a routing slot (broadcast or queue-group).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SlotId(pub u64);

/// A node in the subject routing trie.
///
/// Subjects are dot-separated tokens (e.g. `orders.eu.created`).
/// Wildcards: `*` matches exactly one token, `>` matches one or more trailing tokens.
pub struct TrieNode {
    children: HashMap<Box<str>, TrieNode>,
    /// `*` wildcard child — matches any single token.
    wildcard: Option<Box<TrieNode>>,
    /// `>` tail-match — slots that match this node and everything below.
    tail_match: SmallVec<SlotId, 4>,
    /// Exact subscribers at this node.
    slots: SmallVec<SlotId, 4>,
}

impl TrieNode {
    pub fn new() -> Self {
        Self {
            children: HashMap::new(),
            wildcard: None,
            tail_match: SmallVec::new(),
            slots: SmallVec::new(),
        }
    }

    /// Insert a slot at the position described by `tokens`.
    pub fn insert(&mut self, tokens: &[&str], slot_id: SlotId) {
        match tokens {
            [] => {
                if !self.slots.contains(&slot_id) {
                    self.slots.push(slot_id);
                }
            }
            [">"] => {
                if !self.tail_match.contains(&slot_id) {
                    self.tail_match.push(slot_id);
                }
            }
            ["*", rest @ ..] => {
                let node = self
                    .wildcard
                    .get_or_insert_with(|| Box::new(TrieNode::new()));
                node.insert(rest, slot_id);
            }
            [token, rest @ ..] => {
                let node = self
                    .children
                    .entry(Box::from(*token))
                    .or_insert_with(TrieNode::new);
                node.insert(rest, slot_id);
            }
        }
    }

    /// Recursively remove a slot from all positions in the trie.
    pub fn remove(&mut self, slot_id: SlotId) {
        self.slots.retain(|s| *s != slot_id);
        self.tail_match.retain(|s| *s != slot_id);
        if let Some(ref mut wc) = self.wildcard {
            wc.remove(slot_id);
        }
        for child in self.children.values_mut() {
            child.remove(slot_id);
        }
    }

    /// Find all slots matching a concrete (no-wildcard) subject.
    /// Results are appended to `out`. Callers should clear `out` before calling.
    pub fn lookup(&self, tokens: &[&str], out: &mut Vec<SlotId>) {
        match tokens {
            [] => {
                out.extend_from_slice(&self.slots);
            }
            [token, rest @ ..] => {
                // `>` matches one or more trailing tokens — only when tokens remain.
                out.extend_from_slice(&self.tail_match);
                // Exact child match.
                if let Some(child) = self.children.get(*token) {
                    child.lookup(rest, out);
                }
                // `*` wildcard matches any single token.
                if let Some(ref wc) = self.wildcard {
                    wc.lookup(rest, out);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(trie: &TrieNode, subject: &str) -> Vec<SlotId> {
        let tokens: Vec<&str> = subject.split('.').collect();
        let mut out = Vec::new();
        trie.lookup(&tokens, &mut out);
        out
    }

    #[test]
    fn exact_match() {
        let mut trie = TrieNode::new();
        let s1 = SlotId(1);
        trie.insert(&["orders", "eu", "created"], s1);

        assert_eq!(lookup(&trie, "orders.eu.created"), vec![s1]);
        assert!(lookup(&trie, "orders.eu").is_empty());
        assert!(lookup(&trie, "orders.eu.created.extra").is_empty());
    }

    #[test]
    fn wildcard_single_token() {
        let mut trie = TrieNode::new();
        let s1 = SlotId(1);
        trie.insert(&["orders", "*", "created"], s1);

        assert_eq!(lookup(&trie, "orders.eu.created"), vec![s1]);
        assert_eq!(lookup(&trie, "orders.us.created"), vec![s1]);
        assert!(lookup(&trie, "orders.eu.deleted").is_empty());
        assert!(lookup(&trie, "orders.created").is_empty());
    }

    #[test]
    fn tail_match() {
        let mut trie = TrieNode::new();
        let s1 = SlotId(1);
        trie.insert(&["orders", ">"], s1);

        assert_eq!(lookup(&trie, "orders.eu"), vec![s1]);
        assert_eq!(lookup(&trie, "orders.eu.created"), vec![s1]);
        assert_eq!(lookup(&trie, "orders.us.deleted.v2"), vec![s1]);
        // `>` matches one or more — "orders" alone should not match.
        assert!(lookup(&trie, "orders").is_empty());
    }

    #[test]
    fn combined_wildcards() {
        let mut trie = TrieNode::new();
        let s1 = SlotId(1);
        let s2 = SlotId(2);
        let s3 = SlotId(3);
        trie.insert(&["orders", "eu", "created"], s1);
        trie.insert(&["orders", "*", "created"], s2);
        trie.insert(&["orders", ">"], s3);

        let mut result = lookup(&trie, "orders.eu.created");
        result.sort_by_key(|s| s.0);
        assert_eq!(result, vec![s1, s2, s3]);
    }

    #[test]
    fn remove_slot() {
        let mut trie = TrieNode::new();
        let s1 = SlotId(1);
        let s2 = SlotId(2);
        trie.insert(&["orders", "eu"], s1);
        trie.insert(&["orders", "eu"], s2);

        assert_eq!(lookup(&trie, "orders.eu").len(), 2);

        trie.remove(s1);
        assert_eq!(lookup(&trie, "orders.eu"), vec![s2]);
    }

    #[test]
    fn no_duplicate_insert() {
        let mut trie = TrieNode::new();
        let s1 = SlotId(1);
        trie.insert(&["a", "b"], s1);
        trie.insert(&["a", "b"], s1);

        assert_eq!(lookup(&trie, "a.b"), vec![s1]);
    }

    #[test]
    fn root_tail_match() {
        let mut trie = TrieNode::new();
        let s1 = SlotId(1);
        trie.insert(&[">"], s1);

        assert_eq!(lookup(&trie, "anything"), vec![s1]);
        assert_eq!(lookup(&trie, "a.b.c"), vec![s1]);
    }
}
