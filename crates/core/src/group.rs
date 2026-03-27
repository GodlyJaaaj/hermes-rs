use crate::Subject;
use crate::error::DecodeError;

/// Trait for event groups that aggregate multiple struct-based events.
/// Implemented automatically by the `event_group!` macro.
pub trait EventGroup: Send + Sync + 'static {
    /// All subjects covered by this group.
    fn subjects() -> Vec<Subject>;

    /// Decode a payload based on its subject into the appropriate variant.
    fn decode_event(subject: &Subject, payload: &[u8]) -> Result<Self, DecodeError>
    where
        Self: Sized;
}

/// Macro to create an EventGroup from existing Event structs.
///
/// ```rust,ignore
/// event_group!(OrderEvents = [OrderCreated, OrderShipped]);
/// ```
///
/// Generates:
/// - An enum `OrderEvents` with one variant per type
/// - An `EventGroup` impl with subjects() and decode_event()
#[macro_export]
macro_rules! event_group {
    ($name:ident = [$($event:ident),+ $(,)?]) => {
        #[derive(Debug)]
        pub enum $name {
            $($event($event)),+
        }

        impl $crate::EventGroup for $name {
            fn subjects() -> ::std::vec::Vec<$crate::Subject> {
                let mut subjects = ::std::vec::Vec::new();
                $(subjects.extend(<$event as $crate::Event>::subjects());)+
                subjects
            }

            fn decode_event(subject: &$crate::Subject, payload: &[u8]) -> ::std::result::Result<Self, $crate::DecodeError> {
                $(
                    if <$event as $crate::Event>::subjects().contains(subject) {
                        return Ok(Self::$event($crate::decode::<$event>(payload)?));
                    }
                )+
                Err($crate::DecodeError::UnknownSubject(subject.to_string()))
            }
        }
    };
}
