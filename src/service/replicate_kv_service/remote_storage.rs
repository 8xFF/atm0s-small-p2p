use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    time::Instant,
};

use super::{Action, BroadcastEvent, Changed, Event, KvEvent, NetEvent, RpcEvent, RpcReq, RpcRes, Slot, Version};

#[derive(Debug, PartialEq, Eq)]
enum RemoteStoreState<N, K, V> {
    SyncFull(SyncFullState<N, K, V>),
    Working(WorkingState<N, K, V>),
}

impl<N, K, V> State<N, K, V> for RemoteStoreState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Clone,
    N: Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.init(ctx),
            RemoteStoreState::Working(state) => state.init(ctx),
        }
    }

    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, K, V>, event: BroadcastEvent<K, V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.on_broadcast(ctx, event),
            RemoteStoreState::Working(state) => state.on_broadcast(ctx, event),
        }
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, event: RpcRes<K, V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.on_rpc_res(ctx, event),
            RemoteStoreState::Working(state) => state.on_rpc_res(ctx, event),
        }
    }
}

struct StateCtx<N, K, V> {
    remote: N,
    slots: HashMap<K, Slot<V>>,
    outs: VecDeque<Event<N, K, V>>,
    next_state: Option<RemoteStoreState<N, K, V>>,
}

trait State<N, K, V> {
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>);
    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, K, V>, event: BroadcastEvent<K, V>);
    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, event: RpcRes<K, V>);
}

pub struct RemoteStore<N, K, V> {
    ctx: StateCtx<N, K, V>,
    state: RemoteStoreState<N, K, V>,
    last_active: Instant,
}

impl<N, K, V> RemoteStore<N, K, V>
where
    N: Clone,
    K: Debug + Hash + Ord + Eq + Clone,
    V: Debug + Eq + Clone,
{
    pub fn new(remote: N) -> Self {
        let mut ctx = StateCtx {
            remote,
            slots: HashMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let mut state = SyncFullState::default();
        state.init(&mut ctx);

        Self {
            ctx,
            state: RemoteStoreState::SyncFull(state),
            last_active: Instant::now(),
        }
    }

    pub fn last_active(&self) -> Instant {
        self.last_active
    }

    pub fn on_broadcast(&mut self, event: BroadcastEvent<K, V>) {
        self.last_active = Instant::now();
        self.state.on_broadcast(&mut self.ctx, event);
        if let Some(mut next_state) = self.ctx.next_state.take() {
            next_state.init(&mut self.ctx);
            self.state = next_state;
        }
    }

    pub fn on_rpc_res(&mut self, event: RpcRes<K, V>) {
        self.last_active = Instant::now();
        self.state.on_rpc_res(&mut self.ctx, event);
        if let Some(mut next_state) = self.ctx.next_state.take() {
            next_state.init(&mut self.ctx);
            self.state = next_state;
        }
    }

    pub fn pop_out(&mut self) -> Option<Event<N, K, V>> {
        self.ctx.outs.pop_front()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct SyncFullState<N, K, V> {
    _tmp: PhantomData<(N, K, V)>,
}

impl<N, K, V> Default for SyncFullState<N, K, V> {
    fn default() -> Self {
        Self { _tmp: PhantomData }
    }
}

impl<N, K, V> State<N, K, V> for SyncFullState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Clone,
    N: Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, K, V>) {
        ctx.slots.clear();
        ctx.outs.push_back(Event::NetEvent(NetEvent::Unicast(ctx.remote.clone(), RpcEvent::RpcReq(RpcReq::FetchSnapshot))));
    }

    fn on_broadcast(&mut self, _ctx: &mut StateCtx<N, K, V>, _event: BroadcastEvent<K, V>) {
        // dont process here
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, event: RpcRes<K, V>) {
        match event {
            RpcRes::FetchChanged { .. } => {
                // dont process here
            }
            RpcRes::FetchSnapshot { slots, version } => {
                for (k, slot) in slots.iter() {
                    ctx.outs.push_back(Event::KvEvent(KvEvent::Set(Some(ctx.remote.clone()), k.clone(), slot.value.clone())));
                }
                ctx.slots = slots.into_iter().collect();
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
    _tmp: PhantomData<(N, V)>,
}

impl<N, K, V> WorkingState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Clone,
    N: Clone,
{
    fn new(version: Version) -> Self {
        Self {
            version,
            pendings: BTreeMap::new(),
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
                        ctx.outs.push_back(Event::KvEvent(KvEvent::Set(Some(ctx.remote.clone()), data.key.clone(), value.clone())));
                        ctx.slots.insert(data.key, Slot { version: data.version, value });
                    }
                    Action::Del => {
                        ctx.outs.push_back(Event::KvEvent(KvEvent::Del(Some(ctx.remote.clone()), data.key.clone())));
                        ctx.slots.remove(&data.key);
                    }
                }
            } else {
                ctx.outs.push_back(Event::NetEvent(NetEvent::Unicast(
                    ctx.remote.clone(),
                    RpcEvent::RpcReq(RpcReq::FetchChanged {
                        from: self.version + 1,
                        count: *entry.key() - self.version - 1,
                    }),
                )));
                break;
            }
        }
    }
}

impl<N, K, V> State<N, K, V> for WorkingState<N, K, V>
where
    K: Debug + Hash + Ord + Eq + Clone,
    V: Clone,
    N: Clone,
{
    fn init(&mut self, _ctx: &mut StateCtx<N, K, V>) {
        // dont need init in working state
    }

    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, K, V>, event: BroadcastEvent<K, V>) {
        match event {
            BroadcastEvent::Changed(changed) => {
                if self.version > changed.version {
                    // wrong data => resyncFull
                    ctx.slots.clear();
                    ctx.next_state = Some(RemoteStoreState::SyncFull(SyncFullState::default()));
                } else if self.version < changed.version {
                    self.pendings.insert(changed.version, changed);
                    self.apply_pendings(ctx);
                }
            }
            BroadcastEvent::Version(version) => {
                if version > self.version && self.pendings.is_empty() {
                    // resync part
                    ctx.outs.push_back(Event::NetEvent(NetEvent::Unicast(
                        ctx.remote.clone(),
                        RpcEvent::RpcReq(RpcReq::FetchChanged {
                            from: self.version + 1,
                            count: version - self.version,
                        }),
                    )));
                }
            }
        }
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, K, V>, event: RpcRes<K, V>) {
        match event {
            RpcRes::FetchChanged(Ok(changeds)) => {
                for changed in changeds {
                    if changed.version > self.version {
                        self.pendings.insert(changed.version, changed);
                    }
                }
                self.apply_pendings(ctx);
            }
            RpcRes::FetchChanged(Err(err)) => {
                log::info!("[RemoteStore] fetch changed error: {err:?} => switch to resyncFull");
                ctx.next_state = Some(RemoteStoreState::SyncFull(SyncFullState::default()));
            }
            RpcRes::FetchSnapshot { .. } => {
                // not process here
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// restore with some data
    #[test]
    fn test_restore_full() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: HashMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let mut state = SyncFullState::default();

        state.on_rpc_res(
            &mut ctx,
            RpcRes::FetchSnapshot {
                slots: vec![(1, Slot::new(1, Version(1)))],
                version: Version(1),
            },
        );

        assert_eq!(ctx.slots, HashMap::from([(1, Slot::new(1, Version(1)))]));
        assert_eq!(ctx.next_state, Some(RemoteStoreState::Working(WorkingState::new(Version(1)))));
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// start with zero
    #[test]
    fn test_working_state_zero() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: HashMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(
            &mut ctx,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(1),
            }),
        );

        assert_eq!(ctx.slots, HashMap::from([(1, Slot::new(1, Version(1)))]));
        assert_eq!(ctx.next_state, None);
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// start with zero but got out of sync
    #[test]
    fn test_working_state_zero_out_of_sync() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: HashMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(&mut ctx, BroadcastEvent::Version(Version(1)));

        assert_eq!(ctx.slots, HashMap::new());
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
            slots: HashMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(
            &mut ctx,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(2),
                action: Action::Set(1),
            }),
        );

        assert_eq!(state.pendings.len(), 1);
        assert_eq!(ctx.slots, HashMap::new());
        assert_eq!(ctx.next_state, None);
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(1, RpcEvent::RpcReq(RpcReq::FetchChanged { from: Version(1), count: 1 }))))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        state.on_broadcast(
            &mut ctx,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(2),
            }),
        );

        assert_eq!(state.pendings.len(), 0);
        assert_eq!(ctx.slots, HashMap::from([(1, Slot::new(1, Version(2)))]));
        assert_eq!(ctx.next_state, None);
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 2))));
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);
    }

    /// After missing changed we got FetchChanged response
    #[test]
    fn test_working_state_missing_changed2() {
        let mut ctx: StateCtx<u16, u16, u16> = StateCtx {
            remote: 1,
            slots: HashMap::new(),
            outs: VecDeque::new(),
            next_state: None,
        };

        let mut state = WorkingState::new(Version(0));

        state.on_broadcast(
            &mut ctx,
            BroadcastEvent::Changed(Changed {
                key: 1,
                version: Version(2),
                action: Action::Set(1),
            }),
        );

        assert_eq!(state.pendings.len(), 1);
        assert_eq!(ctx.slots, HashMap::new());
        assert_eq!(ctx.next_state, None);
        assert_eq!(
            ctx.outs.pop_front(),
            Some(Event::NetEvent(NetEvent::Unicast(1, RpcEvent::RpcReq(RpcReq::FetchChanged { from: Version(1), count: 1 }))))
        );
        assert_eq!(ctx.outs.pop_front(), None);

        state.on_rpc_res(
            &mut ctx,
            RpcRes::FetchChanged(Ok(vec![Changed {
                key: 1,
                version: Version(1),
                action: Action::Set(2),
            }])),
        );

        assert_eq!(state.pendings.len(), 0);
        assert_eq!(ctx.slots, HashMap::from([(1, Slot::new(1, Version(2)))]));
        assert_eq!(ctx.next_state, None);
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 2))));
        assert_eq!(ctx.outs.pop_front(), Some(Event::KvEvent(KvEvent::Set(Some(1), 1, 1))));
        assert_eq!(ctx.outs.pop_front(), None);
    }
}
