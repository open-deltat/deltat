//! Per-resource broadcast channels behind LISTEN/NOTIFY.
//!
//! Each resource gets a bounded broadcast ring that fans committed events out to subscribed
//! connections. The bound is deliberate: a slow listener drops old events rather than stalling
//! writers, and is told so with a `Lagged` notification (see `lagged_payload`), so it knows to
//! re-read authoritative state instead of trusting the stream.

use std::sync::Arc;

use dashmap::DashMap;
use serde::Serialize;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::model::{Event, Span};

const CHANNEL_CAPACITY: usize = 256;

/// Why a hold ended, as its notification says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldEnd {
    /// Given back by a DELETE.
    Released,
    /// Removed by the reaper once its expiry passed.
    Expired,
    /// Turned into a booking; the `BookingConfirmed` for `booking_id` follows it.
    Committed,
}

/// What an ended hold or booking's `Event` does not say: when it was, and for a hold why it ended.
/// A subscriber cannot recover these on its own (the interval is gone by the time it hears), and
/// without them a commit reads as "this time is free again" a moment before it reads as booked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Ended {
    pub span: Span,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<HoldEnd>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub booking_id: Option<Ulid>,
}

/// What a LISTEN subscriber is sent for one committed change.
///
/// `Event` is the WAL record format (bincode, no schema version), so it cannot grow fields without
/// making existing logs unreadable. The extra facts ride here, on the notification only.
///
/// The JSON payload is built once, here, and shared by every subscriber's forwarder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub event: Event,
    pub ended: Option<Ended>,
    payload: Arc<str>,
}

/// `{"<Variant>": {"id": .., "resource_id": .., <Ended fields>}}` for an event that ended a hold or
/// booking. Typed structs rather than a `serde_json::Value`, so fields keep their declared order
/// (a Value map sorts keys) and the old fields come first, exactly where they always were.
#[derive(Serialize)]
struct Ending<'a> {
    id: &'a Ulid,
    resource_id: &'a Ulid,
    #[serde(flatten)]
    ended: &'a Ended,
}

#[derive(Serialize)]
enum EndedPayload<'a> {
    HoldReleased(Ending<'a>),
    BookingCancelled(Ending<'a>),
}

fn payload_of(event: &Event, ended: Option<&Ended>) -> Arc<str> {
    let json = match (event, ended) {
        (Event::HoldReleased { id, resource_id }, Some(ended)) => {
            serde_json::to_string(&EndedPayload::HoldReleased(Ending { id, resource_id, ended }))
        }
        (Event::BookingCancelled { id, resource_id }, Some(ended)) => {
            serde_json::to_string(&EndedPayload::BookingCancelled(Ending { id, resource_id, ended }))
        }
        _ => serde_json::to_string(event),
    };
    json.unwrap_or_default().into()
}

impl Notice {
    pub fn of(event: &Event) -> Self {
        Self { event: event.clone(), ended: None, payload: payload_of(event, None) }
    }

    /// `ended` only reaches the payload for the events it describes (`HoldReleased`,
    /// `BookingCancelled`); any other event goes out exactly as `Notice::of` would send it.
    pub fn ended(event: &Event, ended: Ended) -> Self {
        let payload = payload_of(event, Some(&ended));
        Self { event: event.clone(), ended: Some(ended), payload }
    }

    /// The JSON payload: the event in the shape it always had, keys in the same order, with the
    /// `Ended` fields appended inside the variant's object for an ending. A client that does not
    /// know them reads what it always read.
    pub fn payload(&self) -> Arc<str> {
        self.payload.clone()
    }
}

/// Sent instead of silence when a subscriber fell behind and `missed` notifications were dropped,
/// so it knows the stream has a hole and re-reads state rather than trusting what it has.
pub fn lagged_payload(missed: u64) -> String {
    serde_json::json!({ "Lagged": { "missed": missed } }).to_string()
}

/// Broadcast hub for LISTEN/NOTIFY per resource.
pub struct NotifyHub {
    channels: DashMap<Ulid, broadcast::Sender<Notice>>,
}

impl Default for NotifyHub {
    fn default() -> Self {
        Self::new()
    }
}

impl NotifyHub {
    pub fn new() -> Self {
        Self {
            channels: DashMap::new(),
        }
    }

    /// Subscribe to notifications for a resource. Creates the channel if needed.
    pub fn subscribe(&self, resource_id: Ulid) -> broadcast::Receiver<Notice> {
        let sender = self
            .channels
            .entry(resource_id)
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        sender.subscribe()
    }

    /// Send a notification. No-op if nobody is listening.
    pub fn send(&self, resource_id: Ulid, notice: &Notice) {
        if let Some(sender) = self.channels.get(&resource_id) {
            let _ = sender.send(notice.clone());
        }
    }

    /// Remove a channel when its resource is deleted.
    pub fn remove(&self, resource_id: &Ulid) {
        self.channels.remove(resource_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribe_and_receive() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        let mut rx = hub.subscribe(rid);

        let event = Event::ResourceCreated {
            id: rid,
            parent_id: None,
            name: None,
            capacity: 1,
            buffer_after: None,
        };
        hub.send(rid, &Notice::of(&event));

        let received = rx.recv().await.unwrap();
        assert_eq!(received.event, event);
    }

    #[tokio::test]
    async fn send_without_subscribers_is_noop() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        // No subscriber, should not panic
        hub.send(rid, &Notice::of(&Event::ResourceDeleted { id: rid }));
    }

    #[tokio::test]
    async fn multiple_subscribers_all_receive() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        let mut rx1 = hub.subscribe(rid);
        let mut rx2 = hub.subscribe(rid);

        let event = Event::ResourceCreated {
            id: rid,
            parent_id: None,
            name: None,
            capacity: 1,
            buffer_after: None,
        };
        hub.send(rid, &Notice::of(&event));

        let r1 = rx1.recv().await.unwrap();
        let r2 = rx2.recv().await.unwrap();
        assert_eq!(r1.event, event);
        assert_eq!(r2.event, event);
    }

    #[tokio::test]
    async fn remove_channel_stops_delivery() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        let mut rx = hub.subscribe(rid);

        hub.remove(&rid);

        // Channel removed, send is a no-op, receiver gets error
        hub.send(rid, &Notice::of(&Event::ResourceDeleted { id: rid }));

        // The receiver should get an error (channel closed) or lag
        let result = rx.try_recv();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn subscribe_creates_channel_lazily() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();

        // No channel exists yet, subscribe should create it
        let mut rx = hub.subscribe(rid);

        let event = Event::ResourceDeleted { id: rid };
        hub.send(rid, &Notice::of(&event));

        let received = rx.recv().await.unwrap();
        assert_eq!(received.event, event);
    }

    // The payload is a wire contract with every client ever written against deltat, so these pin
    // both halves of it: old fields exactly where they were, new fields only where they are due.

    fn parsed(notice: &Notice) -> serde_json::Value {
        serde_json::from_str(&notice.payload()).unwrap()
    }

    #[test]
    fn a_plain_notice_is_the_event_byte_for_byte() {
        // Compared as bytes, not parsed values: a client may string-match or prefix-scan payloads,
        // and a parsed comparison would not notice keys changing order.
        let rid = Ulid::new();
        let event = Event::HoldPlaced { id: rid, resource_id: rid, span: Span::new(1000, 2000), expires_at: 5000 };
        assert_eq!(&*Notice::of(&event).payload(), serde_json::to_string(&event).unwrap());
    }

    #[test]
    fn an_ending_keeps_the_old_fields_first_in_their_old_order() {
        let (hid, rid) = (Ulid::new(), Ulid::new());
        let event = Event::HoldReleased { id: hid, resource_id: rid };
        let before = serde_json::to_string(&event).unwrap(); // {"HoldReleased":{"id":..,"resource_id":..}}
        let ended = Ended { span: Span::new(1000, 2000), reason: Some(HoldEnd::Released), booking_id: None };
        let payload = Notice::ended(&event, ended).payload();
        let old_prefix = &before[..before.len() - 2]; // without the closing "}}"
        assert!(payload.starts_with(old_prefix), "{payload} must start with {old_prefix}");
        assert!(payload.ends_with(r#","span":{"start":1000,"end":2000},"reason":"released"}}"#), "{payload}");
    }

    #[test]
    fn ended_facts_on_an_event_they_do_not_describe_are_not_sent() {
        let rid = Ulid::new();
        let event = Event::ResourceDeleted { id: rid };
        let ended = Ended { span: Span::new(1000, 2000), reason: None, booking_id: None };
        assert_eq!(&*Notice::ended(&event, ended).payload(), serde_json::to_string(&event).unwrap());
    }

    #[test]
    fn a_hold_that_ended_says_when_and_why_inside_its_own_object() {
        let (hid, rid, bid) = (Ulid::new(), Ulid::new(), Ulid::new());
        let event = Event::HoldReleased { id: hid, resource_id: rid };
        let ended = Ended { span: Span::new(1000, 2000), reason: Some(HoldEnd::Committed), booking_id: Some(bid) };
        let v = parsed(&Notice::ended(&event, ended));
        let inner = &v["HoldReleased"];
        assert_eq!(inner["id"], hid.to_string());
        assert_eq!(inner["resource_id"], rid.to_string());
        assert_eq!(inner["span"], serde_json::json!({ "start": 1000, "end": 2000 }));
        assert_eq!(inner["reason"], "committed");
        assert_eq!(inner["booking_id"], bid.to_string());
        assert_eq!(v.as_object().map(|o| o.len()), Some(1), "still one variant key");
    }

    #[test]
    fn a_cancelled_booking_carries_its_span_and_no_hold_fields() {
        let bid = Ulid::new();
        let event = Event::BookingCancelled { id: bid, resource_id: bid };
        let ended = Ended { span: Span::new(3000, 4000), reason: None, booking_id: None };
        let inner = parsed(&Notice::ended(&event, ended))["BookingCancelled"].clone();
        assert_eq!(inner["span"], serde_json::json!({ "start": 3000, "end": 4000 }));
        assert!(inner.get("reason").is_none());
        assert!(inner.get("booking_id").is_none());
    }

    #[test]
    fn lagged_says_how_many_were_missed() {
        let v: serde_json::Value = serde_json::from_str(&lagged_payload(7)).unwrap();
        assert_eq!(v, serde_json::json!({ "Lagged": { "missed": 7 } }));
    }
}
