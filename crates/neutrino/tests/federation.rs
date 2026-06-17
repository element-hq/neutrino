//! Multi-server federation integration tests, driven by the child-process
//! harness in [`neutrino_testkit`]. Each test spins up real `neutrino` binaries
//! on loopback and exercises real federation HTTP between them — no docker.

use std::path::Path;

use neutrino_testkit::Harness;
use serde_json::Value;

/// Path to the freshly-built `neutrino` binary. Cargo sets `CARGO_BIN_EXE_neutrino`
/// for this crate's integration tests and (re)builds it before they run, so it's
/// never stale.
fn neutrino_bin() -> &'static Path {
    env!("CARGO_BIN_EXE_neutrino").as_ref()
}

/// `m.room.message` body of a timeline event, if it is one.
fn msg_body(e: &Value) -> Option<&str> {
    e.get("content")?.get("body")?.as_str()
}

/// Directed reproduction of the joined-set-growth advertisement
/// (`anti-entropy-extension.md`).
///
/// Background: a server only sends an event to the servers it currently sees as
/// joined. The base anti-entropy mechanism repairs any miss by piggybacking its
/// latest events on the next `/send` to that server — but if there is no next
/// `/send` (the room goes quiet), the miss is permanent. The extension fixes this
/// by sending an advertisement when a server newly becomes joined. This test sets
/// up exactly that permanent miss and checks the advertisement repairs it. Three
/// servers: `H` = holder, `L` = laggard, `R` = resident.
///
/// - `H`, `L`, `R` are all joined to the room.
/// - `L` leaves the room (`$Lv`). `H` and `R` both see the leave.
/// - Cut the `H–R` link.
/// - `H` sets the room name (`$E`). It is sent to no one: `R` is cut off, and `L`
///   already left, so `H`'s fan-out skips it — even though `H–L` is up.
/// - `L` rejoins, via `R`. `R` never saw `$E`, so the rejoin `$J` is built on the
///   old state — `$J` and `$E` are concurrent (neither is built on the other).
/// - Cut the `L–R` link too. Now `L`'s only live link is `H–L`.
/// - Check: `L` still does not have `$E`, even though `H–L` was never cut. The
///   base mechanism did not deliver it (`H` never sent `$E` to `L`, because `L`
///   was not joined when `$E` was created, and the room is now quiet).
/// - Heal the `H–R` link. `H` now receives `L`'s rejoin `$J` and applies it, so
///   `L` becomes joined in `H`'s view.
/// - `L` newly becoming joined, while `H` holds the concurrent `$E`, is what
///   triggers the advertisement: `H` tells `L` its latest events over `H–L` (`L`'s
///   only link). `L` sees it is missing `$E`, fetches it, and applies it.
/// - Check: `L`'s room name is now `$E` — delivered solely by the advertisement.
///   Without the extension this step never happens.
/// - Heal the `L–R` link. All three servers agree the room name is `$E`.
#[tokio::test]
async fn tail_convergence_advertises_on_joined_set_growth() {
    const H: usize = 0;
    const L: usize = 1;
    const R: usize = 2;
    let e_name = "tail-converge-E";

    let h = Harness::start(3, neutrino_bin()).await;
    let room = h.create_room(H, "init").await;
    assert!(matches!(h.invite(H, &room, &h.mxid(L)).await, 200..=299));
    assert!(matches!(h.invite(H, &room, &h.mxid(R)).await, 200..=299));
    assert!(matches!(h.join(L, &room, h.name(H)).await, 200..=299));
    assert!(matches!(h.join(R, &room, h.name(H)).await, 200..=299));
    h.await_converged(&room, &[H, L, R]).await;

    // (1) L leaves; both holders apply it so $E and $J build on the leave.
    assert!(matches!(h.leave(L, &room).await, 200..=299));
    let l_mxid = h.mxid(L);
    h.await_membership(H, &room, &l_mxid, "leave").await;
    h.await_membership(R, &room, &l_mxid, "leave").await;

    // (2) cut H–R; H authors the lone extremity; L rejoins via R (sibling $J).
    h.cut(H, R);
    assert!(matches!(h.set_name(H, &room, e_name).await, 200..=299));
    assert!(matches!(h.join(L, &room, h.name(R)).await, 200..=299));
    h.await_membership(L, &room, &l_mxid, "join").await;
    h.await_membership(R, &room, &l_mxid, "join").await;

    // (3) isolate L to H–L only, so the advertisement is its sole delivery path.
    h.cut(L, R);

    // (4) H–L has been up the whole time, yet L must still lack $E. Pin both
    // sides: H holds $E, L still has the pre-$E name "init" (a readable, specific
    // value — not a vacuous `None`). The partition is genuinely biting.
    assert_eq!(h.current_name(H, &room).await.as_deref(), Some(e_name));
    assert_eq!(
        h.current_name(L, &room).await.as_deref(),
        Some("init"),
        "L should still hold the pre-$E name; if it has $E the base mechanism delivered it"
    );

    // (5) heal H–R: H learns $J, advertises the concurrent $E to L over H–L.
    h.heal(H, R);
    h.await_membership(H, &room, &l_mxid, "join").await; // trigger fired
    h.await_name(L, &room, e_name).await; // <-- the advertisement delivered $E

    // (6) full topology; everyone agrees on the advertised name.
    h.heal(L, R);
    h.await_converged(&room, &[H, L, R]).await;
    for i in [H, L, R] {
        assert_eq!(h.current_name(i, &room).await.as_deref(), Some(e_name));
    }
}

/// Ported from `testrig/smoke.sh` scenario 1: a room, a join, messages flow both
/// ways (asserted via the sliding-sync timeline).
#[tokio::test]
async fn smoke_basic_send_receive() {
    let h = Harness::start(2, neutrino_bin()).await;
    let room = h.create_room(0, "smoke").await;
    assert!(matches!(h.invite(0, &room, &h.mxid(1)).await, 200..=299));
    assert!(matches!(h.join(1, &room, h.name(0)).await, 200..=299));
    h.await_converged(&room, &[0, 1]).await;

    assert!(matches!(
        h.send_message(0, &room, "basic-from-0").await,
        200..=299
    ));
    h.await_timeline(1, &room, "sees 0's message", |e| {
        msg_body(e) == Some("basic-from-0")
    })
    .await;

    assert!(matches!(
        h.send_message(1, &room, "basic-from-1").await,
        200..=299
    ));
    h.await_timeline(0, &room, "sees 1's message", |e| {
        msg_body(e) == Some("basic-from-1")
    })
    .await;
}

/// Ported from `smoke.sh` scenario 2: cut the link, both sides send, heal, both
/// converge. No "let the split settle" sleep is needed: the proxy 503s the cut
/// send so it parks in the outbox immediately, and heal + `KickBackoff` redrains.
#[tokio::test]
async fn smoke_partition_heal_converges() {
    let h = Harness::start(2, neutrino_bin()).await;
    let room = h.create_room(0, "smoke").await;
    assert!(matches!(h.invite(0, &room, &h.mxid(1)).await, 200..=299));
    assert!(matches!(h.join(1, &room, h.name(0)).await, 200..=299));
    h.await_converged(&room, &[0, 1]).await;

    h.cut(0, 1);
    // A local CSAPI write still commits (2xx); only its federation parks.
    assert!(matches!(
        h.send_message(0, &room, "split-from-0").await,
        200..=299
    ));
    assert!(matches!(
        h.send_message(1, &room, "split-from-1").await,
        200..=299
    ));
    h.heal(0, 1);

    h.await_timeline(1, &room, "receives 0's split message after heal", |e| {
        msg_body(e) == Some("split-from-0")
    })
    .await;
    h.await_timeline(0, &room, "receives 1's split message after heal", |e| {
        msg_body(e) == Some("split-from-1")
    })
    .await;
}

/// Ported from `smoke.sh` scenario 3: concurrent room-name resolution across a
/// partition. Server 2 is isolated; servers 0 and 1 (both admins) set conflicting
/// names a few ms apart, so server 1's has the strictly-later `origin_server_ts`.
/// On heal, server 2 must resolve to that later name regardless of arrival order,
/// and must still receive the losing name event into its timeline.
#[tokio::test]
async fn smoke_concurrent_name_resolution() {
    let h = Harness::start(3, neutrino_bin()).await;
    let room = h.create_room(0, "smoke").await;
    for p in [1, 2] {
        assert!(matches!(h.invite(0, &room, &h.mxid(p)).await, 200..=299));
        assert!(matches!(h.join(p, &room, h.name(0)).await, 200..=299));
    }
    h.await_converged(&room, &[0, 1, 2]).await;

    // Server 1 needs admin to author m.room.name. Let the promotion reach both 1
    // and 2 before splitting, or 2 would reject 1's name when it backfills it.
    let one = h.mxid(1);
    assert!(matches!(h.set_power(0, &room, &one, 100).await, 200..=299));
    h.await_power(1, &room, &one, 100).await;
    h.await_power(2, &room, &one, 100).await;

    // Isolate server 2 from both peers; the 0–1 link stays up.
    h.cut(0, 2);
    h.cut(1, 2);

    // Two conflicting names. The ~5ms gap guarantees server 1's event carries the
    // strictly-later `origin_server_ts`, so state-res must pick it.
    assert!(matches!(h.set_name(0, &room, "HS1").await, 200..=299));
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert!(matches!(h.set_name(1, &room, "HS2").await, 200..=299));

    // Heal 1–2 first: server 2 learns HS2 (and backfills the losing HS1).
    h.heal(1, 2);
    h.await_name(2, &room, "HS2").await;

    // Heal 0–2: HS1 now reaches server 2 directly. Resolution is by timestamp, not
    // arrival order, so the name stays HS2 — and HS1 lands in the timeline.
    h.heal(0, 2);
    let zero = h.mxid(0);
    h.await_timeline(
        2,
        &room,
        "timeline contains server 0's losing HS1 name",
        |e| {
            e.get("type").and_then(Value::as_str) == Some("m.room.name")
                && e.get("sender").and_then(Value::as_str) == Some(zero.as_str())
                && e.get("content")
                    .and_then(|c| c.get("name"))
                    .and_then(Value::as_str)
                    == Some("HS1")
        },
    )
    .await;

    h.await_converged(&room, &[0, 1, 2]).await;
    for i in [0, 1, 2] {
        assert_eq!(h.current_name(i, &room).await.as_deref(), Some("HS2"));
    }
}

/// Real-crash durability — the capability the child-process harness exists for.
/// A SIGKILL'd server must recover its committed state *and* redeliver anything
/// parked in its durable outbox. Only a genuine process kill (not an in-process
/// drop) exercises WAL + `synchronous=NORMAL` survival across abrupt termination.
///
/// - Server 0 and 1 are joined and converged.
/// - Cut 0–1, then 0 sends a message. It commits locally on 0 and parks in 0's
///   outbox for 1 (1 is unreachable). That outbox row is the *only* copy headed
///   for 1 — nothing else can carry it.
/// - **SIGKILL server 0** while the message is committed-but-undelivered, then
///   re-spawn it on the same on-disk storage.
/// - Check: 0 still has the message after the abrupt kill (committed state
///   survived), and 1 still does not (the link was never up).
/// - Heal 0–1. 0's recovered outbox redelivers the message to 1 — proving the
///   parked row survived the crash. If durability were broken this times out.
#[tokio::test]
async fn crash_recovers_committed_state_and_redelivers_outbox() {
    let mut h = Harness::start(2, neutrino_bin()).await;
    let room = h.create_room(0, "crash").await;
    assert!(matches!(h.invite(0, &room, &h.mxid(1)).await, 200..=299));
    assert!(matches!(h.join(1, &room, h.name(0)).await, 200..=299));
    h.await_converged(&room, &[0, 1]).await;

    // Park a message in 0's outbox for 1 (link cut → it can't be delivered).
    h.cut(0, 1);
    assert!(matches!(
        h.send_message(0, &room, "pre-crash").await,
        200..=299
    ));

    // Abrupt kill while it's committed-but-undelivered, then revive on the same DB.
    h.crash(0);
    h.revive(0).await;

    // Committed state survived the SIGKILL: 0 still holds its own message.
    h.await_timeline(0, &room, "recovered its own committed message", |e| {
        msg_body(e) == Some("pre-crash")
    })
    .await;
    // 1 still lacks it (0–1 never came up) — so the only path is 0's recovered outbox.
    assert!(
        !h.timeline_has(1, &room, |e| msg_body(e) == Some("pre-crash"))
            .await,
        "server 1 must not have the message before heal — it exists only in 0's recovered outbox"
    );

    // Heal: 0's durable outbox redelivers across the crash boundary.
    h.heal(0, 1);
    h.await_timeline(
        1,
        &room,
        "received the parked message after crash+revive",
        |e| msg_body(e) == Some("pre-crash"),
    )
    .await;
}
