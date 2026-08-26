use crate::engine::*;
use super::helpers::*;

// ══════════════════════════════════════════════════════════════
// Integration vertical: Doctor's Office
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn vertical_doctor_office() {
    let path = test_wal_path("vertical_doctor.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Practice: open 8am-6pm
    let practice = Ulid::new();
    engine.create_resource(practice, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), practice, Span::new(8 * H, 18 * H), false)
        .await
        .unwrap();
    // Lunch break blocked 12-1pm
    engine
        .add_rule(Ulid::new(), practice, Span::new(12 * H, 13 * H), true)
        .await
        .unwrap();

    // Dr. Smith: works 9am-12pm and 1pm-5pm (respects practice lunch block)
    let dr_smith = Ulid::new();
    engine
        .create_resource(dr_smith, Some(practice), None, 1, None)
        .await
        .unwrap();
    engine
        .add_rule(Ulid::new(), dr_smith, Span::new(9 * H, 12 * H), false)
        .await
        .unwrap();
    engine
        .add_rule(Ulid::new(), dr_smith, Span::new(13 * H, 17 * H), false)
        .await
        .unwrap();

    // Dr. Smith's base availability = [9,12) + [13,17)
    let base_avail = engine
        .compute_availability(dr_smith, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(
        base_avail,
        vec![Span::new(9 * H, 12 * H), Span::new(13 * H, 17 * H)]
    );

    // Patient A: 30-min appointment at 9:00
    let patient_a = Ulid::new();
    engine
        .confirm_booking(patient_a, dr_smith, Span::new(9 * H, 9 * H + 30 * M), None)
        .await
        .unwrap();

    // Patient B: 60-min appointment at 14:00
    let patient_b = Ulid::new();
    engine
        .confirm_booking(patient_b, dr_smith, Span::new(14 * H, 15 * H), None)
        .await
        .unwrap();

    // Check: what's still available for a 30-min appointment?
    let avail_30 = engine
        .compute_availability(dr_smith, 0, 24 * H, Some(30 * M))
        .await
        .unwrap();
    // [9:30, 12:00)=150min, [13:00, 14:00)=60min, [15:00, 17:00)=120min, all ≥ 30min
    assert_eq!(avail_30.len(), 3);
    assert_eq!(avail_30[0], Span::new(9 * H + 30 * M, 12 * H));
    assert_eq!(avail_30[1], Span::new(13 * H, 14 * H));
    assert_eq!(avail_30[2], Span::new(15 * H, 17 * H));

    // What's available for a 90-min appointment?
    let avail_90 = engine
        .compute_availability(dr_smith, 0, 24 * H, Some(90 * M))
        .await
        .unwrap();
    // [9:30, 12:00)=150min ✓, [13:00, 14:00)=60min ✗, [15:00, 17:00)=120min ✓
    assert_eq!(avail_90.len(), 2);

    // Doctor calls in sick, add personal blocking
    engine
        .add_rule(Ulid::new(), dr_smith, Span::new(15 * H, 17 * H), true)
        .await
        .unwrap();

    // Cancel patient B (can't come in if doctor leaves early)
    engine.cancel_booking(patient_b).await.unwrap();

    let avail_after_sick = engine
        .compute_availability(dr_smith, 0, 24 * H, None)
        .await
        .unwrap();
    // [9:30, 12:00) + [13:00, 15:00), afternoon cut short
    assert_eq!(avail_after_sick.len(), 2);
    assert_eq!(avail_after_sick[0], Span::new(9 * H + 30 * M, 12 * H));
    assert_eq!(avail_after_sick[1], Span::new(13 * H, 15 * H));
}

// ══════════════════════════════════════════════════════════════
// Integration vertical: Movie Theater
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn vertical_theater_screen_seats() {
    let path = test_wal_path("vertical_theater.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Theater: open 9am-11pm
    let theater = Ulid::new();
    engine.create_resource(theater, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), theater, Span::new(9 * H, 23 * H), false)
        .await
        .unwrap();

    // Screen 1: two showings
    let screen = Ulid::new();
    engine
        .create_resource(screen, Some(theater), None, 1, None)
        .await
        .unwrap();
    // Showing 1: 2pm-4pm (includes 15min cleanup at the end)
    engine
        .add_rule(Ulid::new(), screen, Span::new(14 * H, 16 * H), false)
        .await
        .unwrap();
    // Showing 2: 7pm-9:30pm
    engine
        .add_rule(Ulid::new(), screen, Span::new(19 * H, 21 * H + 30 * M), false)
        .await
        .unwrap();

    // Create 10 seats
    let mut seats = Vec::new();
    for _ in 0..10 {
        let seat = Ulid::new();
        engine.create_resource(seat, Some(screen), None, 1, None).await.unwrap();
        seats.push(seat);
    }

    // Each seat should see both showings
    for &seat_id in &seats {
        let avail = engine
            .compute_availability(seat_id, 0, 24 * H, None)
            .await
            .unwrap();
        assert_eq!(avail.len(), 2);
        assert_eq!(avail[0], Span::new(14 * H, 16 * H));
        assert_eq!(avail[1], Span::new(19 * H, 21 * H + 30 * M));
    }

    // Book 8 of 10 seats for showing 1
    for &seat_id in &seats[..8] {
        engine
            .confirm_booking(Ulid::new(), seat_id, Span::new(14 * H, 16 * H), None)
            .await
            .unwrap();
    }

    // Booked seats have no showing-1 availability, still have showing 2
    let booked_avail = engine
        .compute_availability(seats[0], 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(booked_avail, vec![Span::new(19 * H, 21 * H + 30 * M)]);

    // Unbooked seats still have both
    let free_avail = engine
        .compute_availability(seats[9], 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(free_avail.len(), 2);

    // Theater-level maintenance blocks 7pm-7:30pm
    engine
        .add_rule(Ulid::new(), theater, Span::new(19 * H, 19 * H + 30 * M), true)
        .await
        .unwrap();

    // All seats lose first 30 min of showing 2
    let after_maint = engine
        .compute_availability(seats[9], 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(after_maint.len(), 2);
    assert_eq!(after_maint[0], Span::new(14 * H, 16 * H));
    assert_eq!(after_maint[1], Span::new(19 * H + 30 * M, 21 * H + 30 * M));
}

#[tokio::test]
async fn vertical_theater_sellout_and_cancel() {
    let path = test_wal_path("vertical_sellout_cancel.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let theater = Ulid::new();
    engine.create_resource(theater, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), theater, Span::new(0, 24 * H), false)
        .await
        .unwrap();

    let screen = Ulid::new();
    engine
        .create_resource(screen, Some(theater), None, 1, None)
        .await
        .unwrap();
    engine
        .add_rule(Ulid::new(), screen, Span::new(14 * H, 16 * H), false)
        .await
        .unwrap();

    let mut seats = Vec::new();
    let mut booking_ids = Vec::new();
    for _ in 0..5 {
        let seat = Ulid::new();
        engine.create_resource(seat, Some(screen), None, 1, None).await.unwrap();
        seats.push(seat);

        let bid = Ulid::new();
        engine
            .confirm_booking(bid, seat, Span::new(14 * H, 16 * H), None)
            .await
            .unwrap();
        booking_ids.push(bid);
    }

    // All sold out
    for &seat_id in &seats {
        let avail = engine
            .compute_availability(seat_id, 14 * H, 16 * H, None)
            .await
            .unwrap();
        assert!(avail.is_empty());
    }

    // Cancel seat 0's booking
    engine.cancel_booking(booking_ids[0]).await.unwrap();

    let reopened = engine
        .compute_availability(seats[0], 14 * H, 16 * H, None)
        .await
        .unwrap();
    assert_eq!(reopened, vec![Span::new(14 * H, 16 * H)]);

    // Other seats still sold out
    let still_booked = engine
        .compute_availability(seats[1], 14 * H, 16 * H, None)
        .await
        .unwrap();
    assert!(still_booked.is_empty());
}

// ══════════════════════════════════════════════════════════════
// Integration vertical: Hotel
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn vertical_hotel_multi_night_with_cleaning() {
    let path = test_wal_path("vertical_hotel.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    let day = 24 * H; // 1 day in ms

    // Hotel: always available
    let hotel = Ulid::new();
    engine.create_resource(hotel, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), hotel, Span::new(0, 30 * day), false) // 30 days
        .await
        .unwrap();

    // Room 101
    let room = Ulid::new();
    engine
        .create_resource(room, Some(hotel), None, 1, None)
        .await
        .unwrap();

    // Guest A: 3-night stay (days 5-8, checkout at noon day 8)
    let checkin_a = 5 * day + 14 * H; // 2pm day 5
    let checkout_a = 8 * day + 12 * H; // noon day 8
    let booking_a = Ulid::new();
    engine
        .confirm_booking(booking_a, room, Span::new(checkin_a, checkout_a), None)
        .await
        .unwrap();

    // Cleaning gap: noon-2pm day 8 (blocking rule on room)
    engine
        .add_rule(Ulid::new(), room, Span::new(checkout_a, checkout_a + 2 * H), true)
        .await
        .unwrap();

    // Query day 8: what's available?
    let day8_start = 8 * day;
    let day8_end = 9 * day;
    let avail = engine
        .compute_availability(room, day8_start, day8_end, None)
        .await
        .unwrap();
    // Booking ends at noon. Cleaning noon-2pm. Available 2pm-midnight.
    assert_eq!(avail, vec![Span::new(checkout_a + 2 * H, day8_end)]);

    // Guest B: can book starting 2pm day 8
    engine
        .confirm_booking(Ulid::new(), room, Span::new(checkout_a + 2 * H, 10 * day + 12 * H), None)
        .await
        .unwrap();

    // Guest B can't also book the cleaning slot: the blocking rule closes it for admission
    // exactly as it closes it in the availability view (T-03).
    let result = engine
        .confirm_booking(Ulid::new(), room, Span::new(checkout_a, checkout_a + H), None)
        .await;
    assert!(
        matches!(result, Err(EngineError::ClosedBySchedule { .. })),
        "the cleaning window must reject bookings, got {result:?}"
    );
    let cleaning_avail = engine
        .compute_availability(room, checkout_a, checkout_a + 2 * H, None)
        .await
        .unwrap();
    assert!(cleaning_avail.is_empty());
}

// ══════════════════════════════════════════════════════════════
// Integration vertical: Multi-tenant isolation
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn vertical_multi_tenant_isolation() {
    // Two completely independent resource trees, no cross-contamination
    let path = test_wal_path("vertical_multi_tenant.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Tenant A: gym with rooms
    let gym = Ulid::new();
    engine.create_resource(gym, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), gym, Span::new(6 * H, 22 * H), false) // 6am-10pm
        .await
        .unwrap();

    let yoga_room = Ulid::new();
    engine
        .create_resource(yoga_room, Some(gym), None, 1, None)
        .await
        .unwrap();

    // Tenant B: restaurant with tables
    let restaurant = Ulid::new();
    engine.create_resource(restaurant, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), restaurant, Span::new(11 * H, 23 * H), false) // 11am-11pm
        .await
        .unwrap();

    let table_1 = Ulid::new();
    engine
        .create_resource(table_1, Some(restaurant), None, 1, None)
        .await
        .unwrap();

    // Book yoga room solid
    engine
        .confirm_booking(Ulid::new(), yoga_room, Span::new(6 * H, 22 * H), None)
        .await
        .unwrap();

    // Table should be completely unaffected
    let table_avail = engine
        .compute_availability(table_1, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(table_avail, vec![Span::new(11 * H, 23 * H)]);

    // Can't create cross-tenant child
    let orphan = Ulid::new();
    engine
        .create_resource(orphan, Some(gym), None, 1, None)
        .await
        .unwrap();
    // orphan is under gym, not under restaurant. Totally separate.
    let orphan_avail = engine
        .compute_availability(orphan, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(orphan_avail, vec![Span::new(6 * H, 22 * H)]);
}

// ══════════════════════════════════════════════════════════════
// Integration vertical: Parking Garage
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn vertical_parking_garage() {
    let path = test_wal_path("vertical_parking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Garage: open 6am-midnight
    let garage = Ulid::new();
    engine.create_resource(garage, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), garage, Span::new(6 * H, 24 * H), false)
        .await
        .unwrap();

    // Floor 1: regular parking
    let floor1 = Ulid::new();
    engine
        .create_resource(floor1, Some(garage), None, 1, None)
        .await
        .unwrap();

    // Floor 2: EV only, restricted hours 8am-8pm
    let floor2 = Ulid::new();
    engine
        .create_resource(floor2, Some(garage), None, 1, None)
        .await
        .unwrap();
    engine
        .add_rule(Ulid::new(), floor2, Span::new(8 * H, 20 * H), false)
        .await
        .unwrap();

    // Spots on floor 1 (inherit garage hours 6am-midnight)
    let spot_a = Ulid::new();
    engine
        .create_resource(spot_a, Some(floor1), None, 1, None)
        .await
        .unwrap();

    // Spots on floor 2 (inherit floor2 hours 8am-8pm, overriding garage)
    let ev_spot = Ulid::new();
    engine
        .create_resource(ev_spot, Some(floor2), None, 1, None)
        .await
        .unwrap();

    let spot_a_avail = engine
        .compute_availability(spot_a, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(spot_a_avail, vec![Span::new(6 * H, 24 * H)]);

    let ev_avail = engine
        .compute_availability(ev_spot, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(ev_avail, vec![Span::new(8 * H, 20 * H)]);

    // Park a car in spot_a from 9am-5pm
    engine
        .confirm_booking(Ulid::new(), spot_a, Span::new(9 * H, 17 * H), None)
        .await
        .unwrap();

    // EV spot still fully free
    let ev_after = engine
        .compute_availability(ev_spot, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(ev_after, vec![Span::new(8 * H, 20 * H)]);

    // Floor 1 maintenance 10am-11am (blocks spot_a)
    engine
        .add_rule(Ulid::new(), floor1, Span::new(10 * H, 11 * H), true)
        .await
        .unwrap();

    // spot_a availability: [6,9) already booked out, [17,24) minus floor1 blocking [10,11)
    // Actually: base is garage [6,24) (inherited through floor1 which has no own rules)
    // Minus floor1 blocking [10,11), minus booking [9,17)
    let spot_a_maint = engine
        .compute_availability(spot_a, 0, 24 * H, None)
        .await
        .unwrap();
    // [6,9) + [17,24) but also minus [10,11) which is within booking anyway
    assert_eq!(
        spot_a_maint,
        vec![Span::new(6 * H, 9 * H), Span::new(17 * H, 24 * H)]
    );

    // ev_spot is on floor2, NOT affected by floor1 blocking
    let ev_unaffected = engine
        .compute_availability(ev_spot, 0, 24 * H, None)
        .await
        .unwrap();
    assert_eq!(ev_unaffected, vec![Span::new(8 * H, 20 * H)]);
}

// ══════════════════════════════════════════════════════════════
// Integration vertical: Coworking Space
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn vertical_coworking_space() {
    let path = test_wal_path("vertical_coworking.wal");
    let notify = Arc::new(NotifyHub::new());
    let engine = Engine::new(path, notify).unwrap();

    // Building: open 7am-10pm
    let building = Ulid::new();
    engine.create_resource(building, None, None, 1, None).await.unwrap();
    engine
        .add_rule(Ulid::new(), building, Span::new(7 * H, 22 * H), false)
        .await
        .unwrap();

    // Conference room: bookable in 30-min slots (client-side concern)
    let conf_room = Ulid::new();
    engine
        .create_resource(conf_room, Some(building), None, 1, None)
        .await
        .unwrap();

    // Hot desk area: same hours as building
    let hot_desk = Ulid::new();
    engine
        .create_resource(hot_desk, Some(building), None, 1, None)
        .await
        .unwrap();

    // Morning: 3 conference bookings back to back
    engine
        .confirm_booking(Ulid::new(), conf_room, Span::new(9 * H, 9 * H + 30 * M), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), conf_room, Span::new(9 * H + 30 * M, 10 * H), None)
        .await
        .unwrap();
    engine
        .confirm_booking(Ulid::new(), conf_room, Span::new(10 * H, 10 * H + 30 * M), None)
        .await
        .unwrap();

    // What 30-min slots are left between 8am and 11am?
    let avail = engine
        .compute_availability(conf_room, 8 * H, 11 * H, Some(30 * M))
        .await
        .unwrap();
    // [7,9) clamped to [8,9) = 60min ✓, [10:30, 11) = 30min ✓
    assert_eq!(avail.len(), 2);
    assert_eq!(avail[0], Span::new(8 * H, 9 * H));
    assert_eq!(avail[1], Span::new(10 * H + 30 * M, 11 * H));

    // Building fire drill blocks everything 11am-11:30am
    engine
        .add_rule(Ulid::new(), building, Span::new(11 * H, 11 * H + 30 * M), true)
        .await
        .unwrap();

    // Both rooms affected
    let conf_after = engine
        .compute_availability(conf_room, 11 * H, 12 * H, None)
        .await
        .unwrap();
    assert_eq!(conf_after, vec![Span::new(11 * H + 30 * M, 12 * H)]);

    let desk_after = engine
        .compute_availability(hot_desk, 11 * H, 12 * H, None)
        .await
        .unwrap();
    assert_eq!(desk_after, vec![Span::new(11 * H + 30 * M, 12 * H)]);
}
