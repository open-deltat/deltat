//! Per-resource broadcast channels behind LISTEN/NOTIFY.
//!
//! Each resource gets a bounded broadcast ring that fans committed events out to subscribed
//! connections. The bound is deliberate: a slow listener drops old events rather than stalling
//! writers, then re-reads authoritative state instead of trusting the stream.

use dashmap::DashMap;
use tokio::sync::broadcast;
use ulid::Ulid;

use crate::model::Event;

const CHANNEL_CAPACITY: usize = 256;

/// Broadcast hub for LISTEN/NOTIFY per resource.
pub struct NotifyHub {
    channels: DashMap<Ulid, broadcast::Sender<Event>>,
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
    pub fn subscribe(&self, resource_id: Ulid) -> broadcast::Receiver<Event> {
        let sender = self
            .channels
            .entry(resource_id)
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0);
        sender.subscribe()
    }

    /// Send a notification. No-op if nobody is listening.
    pub fn send(&self, resource_id: Ulid, event: &Event) {
        if let Some(sender) = self.channels.get(&resource_id) {
            let _ = sender.send(event.clone());
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
        hub.send(rid, &event);

        let received = rx.recv().await.unwrap();
        assert_eq!(received, event);
    }

    #[tokio::test]
    async fn send_without_subscribers_is_noop() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        // No subscriber, should not panic
        hub.send(
            rid,
            &Event::ResourceDeleted { id: rid },
        );
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
        hub.send(rid, &event);

        let r1 = rx1.recv().await.unwrap();
        let r2 = rx2.recv().await.unwrap();
        assert_eq!(r1, event);
        assert_eq!(r2, event);
    }

    #[tokio::test]
    async fn remove_channel_stops_delivery() {
        let hub = NotifyHub::new();
        let rid = Ulid::new();
        let mut rx = hub.subscribe(rid);

        hub.remove(&rid);

        // Channel removed, send is a no-op, receiver gets error
        hub.send(rid, &Event::ResourceDeleted { id: rid });

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
        hub.send(rid, &event);

        let received = rx.recv().await.unwrap();
        assert_eq!(received, event);
    }
}
