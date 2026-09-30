//! Per-resource broadcast channels behind LISTEN/NOTIFY.
//!
//! Each resource gets a bounded broadcast ring that fans committed events out to subscribed
//! connections. The bound is deliberate: a slow listener drops old events rather than stalling
//! writers, and is told so with a `Lagged` notification (see `lagged_payload`), so it knows to
//! re-read authoritative state instead of trusting the stream.

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub event: Event,
    pub ended: Option<Ended>,
}

impl Notice {
    pub fn of(event: &Event) -> Self {
        Self { event: event.clone(), ended: None }
    }

    pub fn ended(event: &Event, ended: Ended) -> Self {
        Self { event: event.clone(), ended: Some(ended) }
    }

    /// The JSON payload: the event in exactly the shape it always had, with the `Ended` fields added
    /// inside the variant's object. A client that does not know them reads what it always read.
    pub fn to_payload(&self) -> String {
        let Ok(mut value) = serde_json::to_value(&self.event) else {
            return String::new();
        };
        if let Some(ended) = &self.ended {
            let inner = value
                .as_object_mut()
                .and_then(|outer| outer.values_mut().next())
                .and_then(|v| v.as_object_mut());
            if let (Some(inner), Ok(serde_json::Value::Object(extra))) = (inner, serde_json::to_value(ended)) {
                inner.extend(extra);
            }
        }
        value.to_string()
    }
}

/// Sent instead of silence when a subscriber fell behind the ring and `missed` notifications were
/// dropped, so it knows the stream has a hole and re-reads state rather than trusting what it has.
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
        serde_json::from_str(&notice.to_payload()).unwrap()
    }

    #[test]
    fn a_plain_notice_is_the_event_unchanged() {
        let rid = Ulid::new();
        let event = Event::HoldPlaced { id: rid, resource_id: rid, span: Span::new(1000, 2000), expires_at: 5000 };
        assert_eq!(parsed(&Notice::of(&event)), serde_json::to_value(&event).unwrap());
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
