use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Segment – a single typed piece of a subject (concrete or pattern)
// ---------------------------------------------------------------------------

/// Characters that are reserved and must not appear in string segments.
/// - `.` is the display separator
/// - `*` and `>` are wildcard tokens
const RESERVED_CHARS: &[char] = &['.', '*', '>'];

/// A single segment of a [`Subject`].
///
/// Concrete segments are [`Str`](Segment::Str) and [`Int`](Segment::Int).
/// Pattern segments are [`Any`](Segment::Any) (`*`, matches one) and
/// [`Rest`](Segment::Rest) (`>`, matches zero-or-more trailing).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Segment {
    /// A literal string value.
    Str(String),
    /// A literal integer value.
    Int(i64),
    /// `*` – matches any single segment.
    Any,
    /// `>` – matches zero or more trailing segments. Must be last.
    Rest,
}

impl Segment {
    /// Create a string segment.
    pub fn s(s: impl Into<String>) -> Self {
        Self::Str(s.into())
    }

    /// Create an integer segment.
    pub fn i(v: i64) -> Self {
        Self::Int(v)
    }

    /// Create a single-segment wildcard (`*`).
    pub fn any() -> Self {
        Self::Any
    }

    /// Create a trailing wildcard (`>`).
    pub fn rest() -> Self {
        Self::Rest
    }

    /// `true` if this segment is a wildcard (`Any` or `Rest`).
    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Any | Self::Rest)
    }
}

impl From<&str> for Segment {
    fn from(s: &str) -> Self {
        Self::Str(s.to_owned())
    }
}

impl From<String> for Segment {
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}

impl From<i64> for Segment {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Str(s) => f.write_str(s),
            Self::Int(n) => write!(f, "{n}"),
            Self::Any => f.write_str("*"),
            Self::Rest => f.write_str(">"),
        }
    }
}

// ---------------------------------------------------------------------------
// Subject – a path of segments, both concrete and pattern
// ---------------------------------------------------------------------------

/// A structured subject made of typed [`Segment`]s.
///
/// A subject can be **concrete** (no wildcards) or a **pattern** (with `*`
/// and/or `>`). The wire format is bincode-encoded bytes.
///
/// ```
/// # use hermes_core::Subject;
/// // Concrete:
/// let s = Subject::new().str("job").int(42).str("logs");
/// assert_eq!(s.to_string(), "job.42.logs");
///
/// // Pattern:
/// let p = Subject::new().str("job").any().str("logs");
/// assert_eq!(p.to_string(), "job.*.logs");
///
/// // Roundtrip through bytes:
/// let bytes = s.to_bytes();
/// let s2 = Subject::from_bytes(&bytes).unwrap();
/// assert_eq!(s, s2);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Subject {
    segments: Vec<Segment>,
}

impl Subject {
    /// Create an empty subject.
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    /// Panics if the subject already ends with [`Segment::Rest`].
    fn assert_not_terminated(&self) {
        assert!(
            !matches!(self.segments.last(), Some(Segment::Rest)),
            "cannot append a segment after Rest (>)"
        );
    }

    /// Append a string segment.
    ///
    /// # Panics
    ///
    /// Panics if the string is empty, contains reserved characters, or if
    /// the subject already ends with `>`.
    pub fn str(mut self, s: impl Into<String>) -> Self {
        self.assert_not_terminated();
        let s = s.into();
        assert!(
            !s.is_empty() && !s.contains(RESERVED_CHARS),
            "segment string must not be empty or contain reserved characters (., *, >): {s:?}"
        );
        self.segments.push(Segment::Str(s));
        self
    }

    /// Append an integer segment.
    ///
    /// # Panics
    ///
    /// Panics if the subject already ends with `>`.
    pub fn int(mut self, v: i64) -> Self {
        self.assert_not_terminated();
        self.segments.push(Segment::Int(v));
        self
    }

    /// Append a single-segment wildcard (`*`).
    ///
    /// # Panics
    ///
    /// Panics if the subject already ends with `>`.
    pub fn any(mut self) -> Self {
        self.assert_not_terminated();
        self.segments.push(Segment::Any);
        self
    }

    /// Append a multi-segment wildcard (`>`). Must be the last segment.
    ///
    /// # Panics
    ///
    /// Panics if `>` already exists in the subject.
    pub fn rest(mut self) -> Self {
        assert!(
            !self.segments.iter().any(|s| matches!(s, Segment::Rest)),
            "Rest (>) must be the last segment and cannot appear more than once"
        );
        self.segments.push(Segment::Rest);
        self
    }

    /// Append a raw [`Segment`].
    ///
    /// # Panics
    ///
    /// Panics if the subject already ends with `>`.
    pub fn segment(mut self, seg: Segment) -> Self {
        self.assert_not_terminated();
        self.segments.push(seg);
        self
    }

    /// Access the inner segments.
    pub fn segments(&self) -> &[Segment] {
        &self.segments
    }

    /// Number of segments.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Whether the subject is empty.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// `true` if the subject contains no wildcards.
    ///
    /// Concrete subjects enable fast-path exact-match routing in the broker.
    pub fn is_concrete(&self) -> bool {
        self.segments
            .iter()
            .all(|s| matches!(s, Segment::Str(_) | Segment::Int(_)))
    }

    /// `true` if the subject contains any wildcard segment.
    pub fn is_pattern(&self) -> bool {
        !self.is_concrete()
    }

    /// Test whether a concrete subject matches this pattern.
    ///
    /// If `self` is concrete, this is an equality check.
    pub fn matches(&self, concrete: &Subject) -> bool {
        let pat = &self.segments;
        let subj = &concrete.segments;

        // Fast path: trailing Rest matches zero-or-more.
        if let Some(Segment::Rest) = pat.last() {
            let prefix = &pat[..pat.len() - 1];
            if subj.len() < prefix.len() {
                return false;
            }
            return prefix
                .iter()
                .zip(subj.iter())
                .all(|(p, s)| Self::segment_matches(p, s));
        }

        // Lengths must match exactly.
        if pat.len() != subj.len() {
            return false;
        }

        pat.iter()
            .zip(subj.iter())
            .all(|(p, s)| Self::segment_matches(p, s))
    }

    /// Check if a pattern segment matches a concrete segment.
    fn segment_matches(pattern: &Segment, concrete: &Segment) -> bool {
        match pattern {
            Segment::Any | Segment::Rest => true,
            other => other == concrete,
        }
    }

    /// Serialize to bincode bytes (wire format).
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serde::encode_to_vec(self, bincode::config::standard())
            .expect("Subject serialization cannot fail")
    }

    /// Deserialize from bincode bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, crate::DecodeError> {
        let (val, _) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        Ok(val)
    }

    /// Build a [`Subject`] from `module_path!()` and a type name.
    ///
    /// Splits the module path on `::` and appends each part as a string
    /// segment, then appends the type name.
    pub fn from_module_path(module_path: &str, type_name: &str) -> Self {
        let mut subject = Self::new();
        for part in module_path.split("::") {
            subject.segments.push(Segment::Str(part.to_owned()));
        }
        subject.segments.push(Segment::Str(type_name.to_owned()));
        subject
    }
}

impl Default for Subject {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Subject {
    /// Dot-separated representation: `job.42.logs`, `job.*.>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, seg) in self.segments.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            write!(f, "{seg}")?;
        }
        Ok(())
    }
}

impl From<&str> for Subject {
    /// Parse a dot-separated string. `*` becomes [`Segment::Any`],
    /// `>` becomes [`Segment::Rest`], integers become [`Segment::Int`],
    /// everything else is [`Segment::Str`].
    fn from(s: &str) -> Self {
        let segments = s
            .split('.')
            .filter(|p| !p.is_empty())
            .map(|part| match part {
                "*" => Segment::Any,
                ">" => Segment::Rest,
                _ => part
                    .parse::<i64>()
                    .map(Segment::Int)
                    .unwrap_or_else(|_| Segment::Str(part.to_owned())),
            })
            .collect();
        Self { segments }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Concrete subjects --

    #[test]
    fn subject_builder() {
        let s = Subject::new().str("job").int(42).str("logs");
        assert_eq!(s.segments().len(), 3);
        assert_eq!(s.segments()[0], Segment::Str("job".into()));
        assert_eq!(s.segments()[1], Segment::Int(42));
        assert_eq!(s.segments()[2], Segment::Str("logs".into()));
        assert!(s.is_concrete());
    }

    #[test]
    fn subject_display() {
        let s = Subject::new().str("job").int(42).str("logs");
        assert_eq!(s.to_string(), "job.42.logs");
    }

    #[test]
    fn subject_bytes_roundtrip() {
        let s = Subject::new().str("job").int(42).str("logs");
        let bytes = s.to_bytes();
        let s2 = Subject::from_bytes(&bytes).unwrap();
        assert_eq!(s, s2);
    }

    #[test]
    fn subject_from_str() {
        let s = Subject::from("job.42.logs");
        assert_eq!(s.segments()[0], Segment::Str("job".into()));
        assert_eq!(s.segments()[1], Segment::Int(42));
        assert_eq!(s.segments()[2], Segment::Str("logs".into()));
    }

    #[test]
    fn subject_from_module_path() {
        let s = Subject::from_module_path("app::events", "ChatMessage");
        assert_eq!(s.to_string(), "app.events.ChatMessage");
        let roundtripped = Subject::from_bytes(&s.to_bytes()).unwrap();
        assert_eq!(s, roundtripped);
    }

    // -- Patterns --

    #[test]
    fn pattern_exact_match() {
        let p = Subject::new().str("job").int(42).str("logs");
        let s = Subject::new().str("job").int(42).str("logs");
        assert!(p.matches(&s));
        assert!(p.is_concrete());
    }

    #[test]
    fn pattern_exact_mismatch() {
        let p = Subject::new().str("job").int(42).str("logs");
        let s = Subject::new().str("job").int(99).str("logs");
        assert!(!p.matches(&s));
    }

    #[test]
    fn pattern_any_single() {
        let p = Subject::new().str("job").any().str("logs");
        assert!(p.matches(&Subject::new().str("job").int(42).str("logs")));
        assert!(p.matches(&Subject::new().str("job").str("foo").str("logs")));
        assert!(!p.matches(&Subject::new().str("job").str("logs")));
        assert!(p.is_pattern());
    }

    #[test]
    fn pattern_rest() {
        let p = Subject::new().str("job").rest();
        assert!(p.matches(&Subject::new().str("job")));
        assert!(p.matches(&Subject::new().str("job").int(42)));
        assert!(p.matches(&Subject::new().str("job").int(42).str("logs")));
        assert!(!p.matches(&Subject::new().str("other")));
    }

    #[test]
    fn pattern_rest_with_any() {
        let p = Subject::new().str("a").any().rest();
        assert!(p.matches(&Subject::new().str("a").str("b")));
        assert!(p.matches(&Subject::new().str("a").int(1).str("c").str("d")));
        assert!(!p.matches(&Subject::new().str("a"))); // needs at least 2 segments
    }

    #[test]
    fn pattern_bytes_roundtrip() {
        let p = Subject::new().str("job").any().rest();
        let bytes = p.to_bytes();
        let p2 = Subject::from_bytes(&bytes).unwrap();
        assert_eq!(p, p2);
    }

    #[test]
    fn pattern_mixed_roundtrip() {
        let p = Subject::new().str("a").int(1).any().rest();
        let bytes = p.to_bytes();
        let p2 = Subject::from_bytes(&bytes).unwrap();
        assert_eq!(p, p2);
    }

    #[test]
    fn pattern_display() {
        let p = Subject::new().str("job").any().rest();
        assert_eq!(p.to_string(), "job.*.>");
    }

    #[test]
    fn from_str_wildcards() {
        let s = Subject::from("job.*.>");
        assert_eq!(s.segments()[0], Segment::Str("job".into()));
        assert_eq!(s.segments()[1], Segment::Any);
        assert_eq!(s.segments()[2], Segment::Rest);
        assert!(s.is_pattern());
    }

    #[test]
    fn concrete_is_not_pattern() {
        let s = Subject::new().str("a").int(1);
        assert!(s.is_concrete());
        assert!(!s.is_pattern());
    }

    #[test]
    #[should_panic(expected = "cannot append")]
    fn rest_must_be_last() {
        Subject::new().str("a").rest().str("b");
    }

}
