use serde::{Serialize, de::DeserializeOwned};

use crate::Subject;

/// Trait implemented by all broker events via `#[derive(Event)]`.
///
/// For structs: `subjects()` returns a single subject, `subject()` returns it.
/// For enums: `subjects()` returns one subject per variant, `subject()` matches on the variant.
pub trait Event: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// All subjects this event type can produce.
    /// Structs return 1 subject, enums return N (one per variant).
    fn subjects() -> Vec<Subject>;

    /// The subject of this specific instance.
    fn subject(&self) -> Subject;
}
