use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    time::Instant,
};

use super::{Action, BroadcastEvent, Changed, Event, KvEvent, NetEvent, RpcEvent, RpcReq, RpcRes, Slot, Version};

const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Debug, PartialEq, Eq)]
enum RemoteStoreState<N, K, V> {
    SyncFull(SyncFullState<N, K, V>),
    Working(WorkingState<N, K, V>),
    Destroy(DestroyState<N, K, V>),
}

impl<N, K, V> State<N, K, V> for RemoteStoreState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Debug + Clone,
    N: Debug + Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant) {
        match self {
            RemoteStoreState::SyncFull(state) => state.init(ctx, now),
            RemoteStoreState::Working(state) => state.init(ctx, now),
            RemoteStoreState::Destroy(state) => state.init(ctx, now),
        }
    }

    fn on_tick(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant) {
        match self {
            RemoteStoreState::SyncFull(state) => state.on_tick(ctx, now),
            RemoteStoreState::Working(state) => state.on_tick(ctx, now),
            RemoteStoreState::Destroy(state) => state.on_tick(ctx, now),
        }
    }

    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant, event: BroadcastEvent<K, V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.on_broadcast(ctx, now, event),
            RemoteStoreState::Working(state) => state.on_broadcast(ctx, now, event),
            RemoteStoreState::Destroy(state) => state.on_broadcast(ctx, now, event),
        }
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant, event: RpcRes<K, V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.on_rpc_res(ctx, now, event),
            RemoteStoreState::Working(state) => state.on_rpc_res(ctx, now, event),
            RemoteStoreState::Destroy(state) => state.on_rpc_res(ctx, now, event),
        }
    }
}

struct StateCtx<N, K, V> {
    remote: N,
    slots: BTreeMap<K, Slot<V>>,
    outs: VecDeque<Event<N, K, V>>,
    next_state: Option<RemoteStoreState<N, K, V>>,
}

trait State<N, K, V> {
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant);
    fn on_tick(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant);
    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant, event: BroadcastEvent<K, V>);
    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant, event: RpcRes<K, V>);
}

pub struct RemoteStore<N, K, V> {
    ctx: StateCtx<N, K, V>,
    state: RemoteStoreState<N, K, V>,
    last_active: Instant,
}

impl<N, K, V> RemoteStore<N, K, V>
where
    N: Debug + Clone,
    K: Debug + Hash + Ord + Eq + Clone,
    V: Debug + Eq + Clone,
{
    pub fn new(remote: N) -> Self {
        let mut ctx = StateCtx {
            remote,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let mut state = SyncFullState::default();
        state.init(&mut ctx, Instant::now());

        Self {
            ctx,
            state: RemoteStoreState::SyncFull(state),
            last_active: Instant::now(),
        }
    }

    pub fn on_tick(&mut self) {
        self.state.on_tick(&mut self.ctx, Instant::now());
    }

    pub fn destroy(&mut self) {
        let mut state = DestroyState { _tmp: PhantomData };
        state.init(&mut self.ctx, Instant::now());
        self.state = RemoteStoreState::Destroy(state);
    }

    pub fn last_active(&self) -> Instant {
        self.last_active
    }

    pub fn on_broadcast(&mut self, event: BroadcastEvent<K, V>) {
        self.last_active = Instant::now();
        self.state.on_broadcast(&mut self.ctx, Instant::now(), event);
        if let Some(mut next_state) = self.ctx.next_state.take() {
            next_state.init(&mut self.ctx, Instant::now());
            self.state = next_state;
        }
    }

    pub fn on_rpc_res(&mut self, event: RpcRes<K, V>) {
        self.last_active = Instant::now();
        self.state.on_rpc_res(&mut self.ctx, Instant::now(), event);
        if let Some(mut next_state) = self.ctx.next_state.take() {
            next_state.init(&mut self.ctx, Instant::now());
            self.state = next_state;
        }
    }

    pub fn pop_out(&mut self) -> Option<Event<N, K, V>> {
        self.ctx.outs.pop_front()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SyncFullState<N, K, V> {
    version: Option<Version>,
    biggest_key: Option<K>,
    sending_req: Option<(Instant, NetEvent<N, K, V>)>,
    _tmp: PhantomData<(N, K, V)>,
}

impl<N, K, V> Default for SyncFullState<N, K, V> {
    fn default() -> Self {
        Self {
            version: None,
            biggest_key: None,
            sending_req: None,
            _tmp: PhantomData,
        }
    }
}

impl<N, K, V> State<N, K, V> for SyncFullState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Debug + Clone,
    N: Debug + Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant) {
        log::info!("[RemoteStore {:?}] switch to syncFull", ctx.remote);
        while let Some((k, _v)) = ctx.slots.pop_first() {
            ctx.outs.push_back(Event::KvEvent(KvEvent::Del(Some(ctx.remote.clone()), k)));
        }
        // first time we don't have information about data then request snapshot without from and to
        // after it response, we will request snapshot with from and to if needed
        let req = NetEvent::Unicast(
            ctx.remote.clone(),
            RpcEvent::RpcReq(RpcReq::FetchSnapshot {
                from: None,
                to: None,
                max_version: None,
            }),
        );
        self.sending_req = Some((now, req.clone()));
        ctx.outs.push_back(Event::NetEvent(req));
    }

    fn on_tick(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant) {
        if let Some((sent_at, req)) = self.sending_req.as_mut() {
            let now = now;
            if now - *sent_at >= REQUEST_TIMEOUT {
                log::warn!("[RemoteStore {:?}] syncFull timeout => resend", ctx.remote);
                *sent_at = now;
                ctx.outs.push_back(Event::NetEvent(req.clone()));
            }
        }
    }

    fn on_broadcast(&mut self, _ctx: &mut StateCtx<N, K, V>, _now: Instant, _event: BroadcastEvent<K, V>) {
        // dont process here
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant, event: RpcRes<K, V>) {
        match event {
            RpcRes::FetchChanged { .. } => {
                // dont process here
            }
            RpcRes::FetchSnapshot(Some(snapshot), version) => {
                // TODO check snapshot is not empty
                log::info!(
                    "[RemoteStore {:?}] got snapshot {} slots and biggest_key {:?}, current version {version:?}, next {:?}",
                    ctx.remote,
                    snapshot.slots.len(),
                    snapshot.biggest_key,
                    snapshot.next_key,
                );
                for (k, slot) in snapshot.slots.into_iter() {
                    ctx.outs.push_back(Event::KvEvent(KvEvent::Set(Some(ctx.remote.clone()), k.clone(), slot.value.clone())));
                    ctx.slots.insert(k, slot);
                }
                if self.version.is_none() {
                    self.version = Some(version);
                    self.biggest_key = Some(snapshot.biggest_key);
                }
                if let Some(next_key) = snapshot.next_key {
                    let to = self.biggest_key.clone().expect("should have biggest key");
                    let max_version = self.version.expect("should have version");

                    log::info!(
                        "[RemoteStore {:?}] request more snapshot data with from {next_key:?} and to {to:?}, max_version {max_version:?}",
                        ctx.remote
                    );
                    let req = NetEvent::Unicast(
                        ctx.remote.clone(),
                        RpcEvent::RpcReq(RpcReq::FetchSnapshot {
                            from: Some(next_key),
                            to: Some(to),
                            max_version: Some(max_version),
                        }),
                    );
                    self.sending_req = Some((now, req.clone()));
                    ctx.outs.push_back(Event::NetEvent(req));
                } else {
                    let version = self.version.expect("should have version");
                    log::info!("[RemoteStore {:?}] switch to working with {} slots and version {version:?}", ctx.remote, ctx.slots.len());
                    self.sending_req = None;
                    ctx.next_state = Some(RemoteStoreState::Working(WorkingState::new(version)));
                }
            }
            RpcRes::FetchSnapshot(None, version) => {
                log::info!("[RemoteStore {:?}] switch to working with 0 slots and version {version:?}", ctx.remote);
                self.sending_req = None;
                ctx.next_state = Some(RemoteStoreState::Working(WorkingState::new(version)));
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct WorkingState<N, K, V> {
    version: Version,
    // this is a list of changeds in order, we use it to detect discontinuity to send fetchChanged
    pendings: BTreeMap<Version, Changed<K, V>>,
    sending_req: Option<(Instant, NetEvent<N, K, V>)>,
    _tmp: PhantomData<(N, V)>,
}

impl<N, K, V> WorkingState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Debug + Clone,
    N: Debug + Clone,
{
    fn new(version: Version) -> Self {
        Self {
            version,
            pendings: BTreeMap::new(),
            sending_req: None,
            _tmp: PhantomData,
        }
    }

    fn apply_pendings(&mut self, ctx: &mut StateCtx<N, K, V>) {
        while let Some(entry) = self.pendings.first_entry() {
            if *entry.key() == self.version + 1 {
                self.version = self.version + 1;
                let (_, data) = self.pendings.pop_first().expect("should have data");
                match data.action {
                    Action::Set(value) => {
                        log::debug!("[RemoteStore {:?}] apply set k {:?} => version {:?}", ctx.remote, data.key, data.version);
                        ctx.outs.push_back(Event::KvEvent(KvEvent::Set(Some(ctx.remote.clone()), data.key.clone(), value.clone())));
                        ctx.slots.insert(data.key, Slot { version: data.version, value });
                    }
                    Action::Del => {
                        log::debug!("[RemoteStore {:?}] apply del k {:?} => version {:?}", ctx.remote, data.key, data.version);
                        ctx.outs.push_back(Event::KvEvent(KvEvent::Del(Some(ctx.remote.clone()), data.key.clone())));
                        ctx.slots.remove(&data.key);
                    }
                }
            } else {
                let from = self.version + 1;
                let count = *entry.key() - self.version - 1;
                log::warn!("[RemoteStore {:?}] apply pendings discontinuity => request fetch changed from {from:?} count {count}", ctx.remote);
                let req = NetEvent::Unicast(ctx.remote.clone(), RpcEvent::RpcReq(RpcReq::FetchChanged { from, count }));
                self.sending_req = Some((Instant::now(), req.clone()));
                ctx.outs.push_back(Event::NetEvent(req));
                break;
            }
        }
    }
}

impl<N, K, V> State<N, K, V> for WorkingState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Debug + Clone,
    N: Debug + Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>, _now: Instant) {
        // dont need init in working state
        log::info!("[RemoteStore {:?}] switch to working", ctx.remote);
    }

    fn on_tick(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant) {
        if let Some((sent_at, req)) = self.sending_req.as_mut() {
            if now - *sent_at >= REQUEST_TIMEOUT {
                log::warn!("[RemoteStore {:?}] fetch changed timeout", ctx.remote);
                *sent_at = now;
                ctx.outs.push_back(Event::NetEvent(req.clone()));
            }
        }
    }

    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, K, V>, now: Instant, event: BroadcastEvent<K, V>) {
        match event {
            BroadcastEvent::Changed(changed) => {
                if self.version < changed.version {
                    self.pendings.insert(changed.version, changed);
                    self.apply_pendings(ctx);
                }
            }
            BroadcastEvent::Version(version) => {
                if version > self.version && self.pendings.is_empty() {
                    // resync part
                    let from = self.version + 1;
                    let count = version - self.version;
                    log::warn!("[RemoteStore {:?}] received discontinuity version => request fetch changed from {from:?} count {count}", ctx.remote);
                    let req = NetEvent::Unicast(ctx.remote.clone(), RpcEvent::RpcReq(RpcReq::FetchChanged { from, count }));
                    self.sending_req = Some((now, req.clone()));
                    ctx.outs.push_back(Event::NetEvent(req));
                }
            }
        }
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, _now: Instant, event: RpcRes<K, V>) {
        match event {
            RpcRes::FetchChanged(Ok(changeds)) => {
                log::info!("[RemoteStore {:?}] fetch changed success with {} changeds => apply", ctx.remote, changeds.len());
                for changed in changeds {
                    if changed.version > self.version {
                        self.pendings.insert(changed.version, changed);
                    }
                }
                self.sending_req = None;
                self.apply_pendings(ctx);
            }
            RpcRes::FetchChanged(Err(err)) => {
                log::info!("[RemoteStore] fetch changed error: {err:?} => switch to resyncFull");
                self.sending_req = None;
                ctx.next_state = Some(RemoteStoreState::SyncFull(SyncFullState::default()));
            }
            RpcRes::FetchSnapshot { .. } => {
                // not process here
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct DestroyState<N, K, V> {
    _tmp: PhantomData<(N, K, V)>,
}

impl<N, K, V> State<N, K, V> for DestroyState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Clone,
    N: Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>, _now: Instant) {
        while let Some((k, _v)) = ctx.slots.pop_first() {
            ctx.outs.push_back(Event::KvEvent(KvEvent::Del(Some(ctx.remote.clone()), k)));
        }
    }

    fn on_tick(&mut self, _ctx: &mut StateCtx<N, K, V>, _now: Instant) {
        // dont process here
    }

    fn on_broadcast(&mut self, _ctx: &mut StateCtx<N, K, V>, _now: Instant, _event: BroadcastEvent<K, V>) {
        // dont process here
    }

    fn on_rpc_res(&mut self, _ctx: &mut StateCtx<N, K, V>, _now: Instant, _event: RpcRes<K, V>) {
        // dont process here
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::replicate_kv_service::SnapshotData;

    use super::*;

    /// restore with some data
    #[test]
    fn test_restore_full_single_pkt() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = SyncFullState::default();
        state.init(&mut ctx, now);

        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(
                1,
                RpcEvent::RpcReq(RpcReq::FetchSnapshot {
                    from: None,
                    to: None,
                    max_version: None
                })
            )))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        // we don't need to resend again because it too fast
        state.on_tick(&mut ctx, now);
        assert_eq!(ctx.outs.pop_front(), None);

        state.on_rpc_res(
            &mut ctx,
            now,
            RpcRes::FetchSnapshot(
                Some(SnapshotData {
                    slots: vec![(1, Slot::new(1, Version(1)))],
                    next_key: None,
                    biggest_key: 1,
                }),
                Version(1),
            ),
        );

        assert_eq!(ctx.slots, BTreeMap::from([(1, Slot::new(1, Version(1)))]));
        assert_eq!(ctx.next_state, Some(RemoteStoreState::Working(WorkingState::new(Version(1)))));
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);

        // we don't need to resend again because it already received answer
        state.on_tick(&mut ctx, now + REQUEST_TIMEOUT + Duration::from_millis(1));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    #[test]
    fn test_restore_full_resend() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = SyncFullState::default();
        state.init(&mut ctx, now);

        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(
                1,
                RpcEvent::RpcReq(RpcReq::FetchSnapshot {
                    from: None,
                    to: None,
                    max_version: None
                })
            )))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        // we need to resend again because it timeout
        state.on_tick(&mut ctx, now + REQUEST_TIMEOUT + Duration::from_millis(1));
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(
                1,
                RpcEvent::RpcReq(RpcReq::FetchSnapshot {
                    from: None,
                    to: None,
                    max_version: None
                })
            )))
        );
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// restore with some data
    #[test]
    fn test_restore_multi_single_pkt() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = SyncFullState::default();
        state.init(&mut ctx, now);
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(
                1,
                RpcEvent::RpcReq(RpcReq::FetchSnapshot {
                    from: None,
                    to: None,
                    max_version: None
                })
            )))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        // got first sync
        state.on_rpc_res(
            &mut ctx,
            now,
            RpcRes::FetchSnapshot(
                Some(SnapshotData {
                    slots: vec![(1, Slot::new(1, Version(1)))],
                    next_key: Some(2),
                    biggest_key: 2,
                }),
                Version(2),
            ),
        );

        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(
                1,
                RpcEvent::RpcReq(RpcReq::FetchSnapshot {
                    from: Some(2),
                    to: Some(2),
                    max_version: Some(Version(2))
                })
            )))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        // got last sync
        state.on_rpc_res(
            &mut ctx,
            now,
            RpcRes::FetchSnapshot(
                Some(SnapshotData {
                    slots: vec![(2, Slot::new(2, Version(2)))],
                    next_key: None,
                    biggest_key: 2,
                }),
                Version(2),
            ),
        );

        assert_eq!(ctx.slots, BTreeMap::from([(1, Slot::new(1, Version(1))), (2, Slot::new(2, Version(2)))]));
        assert_eq!(ctx.next_state, Some(RemoteStoreState::Working(WorkingState::new(Version(2)))));
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 2, 2))));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// start with zero
    #[test]
    fn test_working_state_zero() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(
            &mut ctx,
            now,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(1),
            }),
        );

        assert_eq!(ctx.slots, BTreeMap::from([(1, Slot::new(1, Version(1)))]));
        assert_eq!(ctx.next_state, None);
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// start with zero but got out of sync
    #[test]
    fn test_working_state_zero_out_of_sync() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(&mut ctx, now, BroadcastEvent::Version(Version(1)));

        assert_eq!(ctx.slots, BTreeMap::new());
        assert_eq!(ctx.next_state, None);
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(1, RpcEvent::RpcReq(RpcReq::FetchChanged { from: Version(1), count: 1 }))))
        );
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// After missing changed we got Changed event
    #[test]
    fn test_working_state_missing_changed() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(
            &mut ctx,
            now,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(2),
                action: Action::Set(1),
            }),
        );

        assert_eq!(state.pendings.len(), 1);
        assert_eq!(ctx.slots, BTreeMap::new());
        assert_eq!(ctx.next_state, None);
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(1, RpcEvent::RpcReq(RpcReq::FetchChanged { from: Version(1), count: 1 }))))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        state.on_broadcast(
            &mut ctx,
            now,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(2),
            }),
        );

        assert_eq!(state.pendings.len(), 0);
        assert_eq!(ctx.slots, BTreeMap::from([(1, Slot::new(1, Version(2)))]));
        assert_eq!(ctx.next_state, None);
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 2))));
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);

        // after received FetchChanged, it should be rejected
        state.on_rpc_res(
            &mut ctx,
            now,
            RpcRes::FetchChanged(Ok(vec![Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(2),
            }])),
        );
        assert_eq!(ctx.outs.pop_front(), None);

        // after received FetchChanged it should not be resend
        state.on_tick(&mut ctx, now + REQUEST_TIMEOUT + Duration::from_millis(1));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// After missing changed we got FetchChanged response
    #[test]
    fn test_working_state_missing_changed2() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(
            &mut ctx,
            now,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(2),
                action: Action::Set(1),
            }),
        );

        assert_eq!(state.pendings.len(), 1);
        assert_eq!(ctx.slots, BTreeMap::new());
        assert_eq!(ctx.next_state, None);
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(1, RpcEvent::RpcReq(RpcReq::FetchChanged { from: Version(1), count: 1 }))))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        state.on_rpc_res(
            &mut ctx,
            now,
            RpcRes::FetchChanged(Ok(vec![Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(2),
            }])),
        );

        assert_eq!(state.pendings.len(), 0);
        assert_eq!(ctx.slots, BTreeMap::from([(1, Slot::new(1, Version(2)))]));
        assert_eq!(ctx.next_state, None);
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 2))));
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    #[test]
    fn test_working_state_resend_timeout_fetch_changed() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(
            &mut ctx,
            now,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(2),
                action: Action::Set(1),
            }),
        );

        assert_eq!(state.pendings.len(), 1);
        assert_eq!(ctx.slots, BTreeMap::new());
        assert_eq!(ctx.next_state, None);
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(1, RpcEvent::RpcReq(RpcReq::FetchChanged { from: Version(1), count: 1 }))))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        // now after timeout we should resend
        state.on_tick(&mut ctx, now + REQUEST_TIMEOUT + Duration::from_millis(1));
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(1, RpcEvent::RpcReq(RpcReq::FetchChanged { from: Version(1), count: 1 }))))
        );
        assert_eq!(ctx.outs.pop_front(), None);
    }

    #[test]
    fn destroy_remote_should_clear_slots() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: BTreeMap::from([(1, Slot::new(1, Version(1)))]),
            outs: VecDeque::new(),
            next_state: None,
        };

        let now = Instant::now();
        let mut state = DestroyState { _tmp: PhantomData };
        state.init(&mut ctx, now);

        assert_eq!(ctx.slots, BTreeMap::new());
        assert_eq!(ctx.next_state, None);
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Del(Some(1), 1))));
        assert_eq!(ctx.outs.pop_front(), None);
    }
}
