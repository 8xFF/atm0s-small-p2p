use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fmt::Debug,
    marker::PhantomData,
};

use super::{Action, BroadcastEvent, Changed, Key, NetEvent, RpcReq, RpcRes, Slot, Version};

#[derive(Debug)]
pub enum RemoteStoreState<N, V> {
    SyncFull(SyncFullState<N, V>),
    SyncPart(SyncPartState<N, V>),
    Working(WorkingState<N, V>),
}

impl<N, V> State<N, V> for RemoteStoreState<N, V>
where
    N: Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.init(ctx),
            RemoteStoreState::SyncPart(state) => state.init(ctx),
            RemoteStoreState::Working(state) => state.init(ctx),
        }
    }

    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, V>, event: BroadcastEvent<V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.on_broadcast(ctx, event),
            RemoteStoreState::SyncPart(state) => state.on_broadcast(ctx, event),
            RemoteStoreState::Working(state) => state.on_broadcast(ctx, event),
        }
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, V>, event: RpcRes<V>) {
        match self {
            RemoteStoreState::SyncFull(state) => state.on_rpc_res(ctx, event),
            RemoteStoreState::SyncPart(state) => state.on_rpc_res(ctx, event),
            RemoteStoreState::Working(state) => state.on_rpc_res(ctx, event),
        }
    }
}

struct StateCtx<N, V> {
    remote: N,
    slots: HashMap<Key, Slot<V>>,
    outs: VecDeque<NetEvent<N, V>>,
    next_state: Option<RemoteStoreState<N, V>>,
}

trait State<N, V> {
    fn init(&mut self, ctx: &mut StateCtx<N, V>);
    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, V>, event: BroadcastEvent<V>);
    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, V>, event: RpcRes<V>);
}

pub struct RemoteStore<N, V> {
    ctx: StateCtx<N, V>,
    state: RemoteStoreState<N, V>,
}

impl<N, V> RemoteStore<N, V>
where
    N: Clone,
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
        }
    }

    pub fn on_broadcast(&mut self, event: BroadcastEvent<V>) {
        self.state.on_broadcast(&mut self.ctx, event);
        if let Some(mut next_state) = self.ctx.next_state.take() {
            next_state.init(&mut self.ctx);
            self.state = next_state;
        }
    }

    pub fn on_rpc_res(&mut self, event: RpcRes<V>) {
        self.state.on_rpc_res(&mut self.ctx, event);
        if let Some(mut next_state) = self.ctx.next_state.take() {
            next_state.init(&mut self.ctx);
            self.state = next_state;
        }
    }

    pub fn pop_out(&mut self) -> Option<NetEvent<N, V>> {
        self.ctx.outs.pop_front()
    }
}

#[derive(Debug)]
struct SyncFullState<N, V> {
    _tmp: PhantomData<(N, V)>,
}

impl<N, V> Default for SyncFullState<N, V> {
    fn default() -> Self {
        Self { _tmp: PhantomData }
    }
}

impl<N, V> State<N, V> for SyncFullState<N, V>
where
    N: Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, V>) {
        ctx.slots.clear();
        ctx.outs.push_back(NetEvent::RpcReq(ctx.remote.clone(), RpcReq::FetchSnapshot));
    }

    fn on_broadcast(&mut self, _ctx: &mut StateCtx<N, V>, _event: BroadcastEvent<V>) {
        // dont process here
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, V>, event: RpcRes<V>) {
        match event {
            RpcRes::FetchChanged { .. } => {
                // dont process here
            }
            RpcRes::FetchSnapshot { slots, version } => {
                ctx.slots = slots.into_iter().collect();
                ctx.next_state = Some(RemoteStoreState::Working(WorkingState::new(version)));
            }
        }
    }
}

#[derive(Debug)]
struct SyncPartState<N, V> {
    from_version: Version,
    pendings: BTreeMap<Version, (Vec<Changed<V>>, bool)>,
    _tmp: PhantomData<(N, V)>,
}

impl<N, V> SyncPartState<N, V> {
    fn new(from_version: Version) -> Self {
        Self {
            from_version,
            pendings: BTreeMap::new(),
            _tmp: PhantomData,
        }
    }
}

impl<N, V> SyncPartState<N, V> {
    /// we check if pendings list is continuos and start with from_version and end with remain is false
    fn is_finished(&self) -> bool {
        if self.pendings.is_empty() {
            return false;
        }

        let mut coniguous = true;
        let mut last_version = self.from_version;
        for (version, (changes, _remain)) in &self.pendings {
            if *version != last_version {
                coniguous = false;
                break;
            }
            last_version = changes.last().expect("must have last entry").version + 1;
        }
        // check if data is coniguous and last entry is finished
        coniguous && self.pendings.last_key_value().expect("must have last entry").1 .1 == false
    }
}

impl<N, V> State<N, V> for SyncPartState<N, V>
where
    N: Clone,
{
    fn init(&mut self, ctx: &mut StateCtx<N, V>) {
        ctx.outs.push_back(NetEvent::RpcReq(ctx.remote.clone(), RpcReq::FetchChanged { from: self.from_version, to: None }));
    }

    fn on_broadcast(&mut self, _ctx: &mut StateCtx<N, V>, _event: BroadcastEvent<V>) {
        // dont process here
    }

    fn on_rpc_res(&mut self, ctx: &mut StateCtx<N, V>, event: RpcRes<V>) {
        match event {
            RpcRes::FetchChanged { changes, remain } => {
                if let Some(changed) = changes.first() {
                    self.pendings.insert(changed.version, (changes, remain));
                    if self.is_finished() {
                        let last_version = self.pendings.last_key_value().expect("must have last entry").1 .0.last().expect("must have last entry").version;
                        while let Some((_version, (changes, _remain))) = self.pendings.pop_first() {
                            for changed in changes {
                                match changed.action {
                                    Action::Set(value) => {
                                        ctx.slots.insert(changed.key, Slot { version: changed.version, value });
                                    }
                                    Action::Del => {
                                        ctx.slots.remove(&changed.key);
                                    }
                                }
                            }
                        }
                        ctx.next_state = Some(RemoteStoreState::Working(WorkingState::new(last_version)));
                    }
                } else {
                    log::warn!("[RemoteStore:SyncPartState] empty changes received");
                }
            }
            RpcRes::FetchSnapshot { .. } => {
                // dont process here
            }
        }
    }
}

#[derive(Debug)]
struct WorkingState<N, V> {
    version: Version,
    _tmp: PhantomData<(N, V)>,
}

impl<N, V> WorkingState<N, V> {
    fn new(version: Version) -> Self {
        Self { version, _tmp: PhantomData }
    }
}

impl<N, V> State<N, V> for WorkingState<N, V> {
    fn init(&mut self, _ctx: &mut StateCtx<N, V>) {
        // dont need init in working state
    }

    fn on_broadcast(&mut self, ctx: &mut StateCtx<N, V>, event: BroadcastEvent<V>) {
        match event {
            BroadcastEvent::Changed(changed) => {
                if self.version > changed.version {
                    // wrong data => resyncFull
                    ctx.slots.clear();
                    ctx.next_state = Some(RemoteStoreState::SyncFull(SyncFullState::default()));
                } else {
                    let delta = changed.version - self.version;
                    if delta == 1 {
                        // correct version
                        self.version = changed.version;
                        match changed.action {
                            Action::Set(value) => {
                                ctx.slots.insert(changed.key, Slot { version: changed.version, value });
                            }
                            Action::Del => {
                                ctx.slots.remove(&changed.key);
                            }
                        }
                    } else {
                        // resync part
                        ctx.next_state = Some(RemoteStoreState::SyncPart(SyncPartState::new(self.version + 1)));
                    }
                }
            }
            BroadcastEvent::Version(version) => {
                if version > self.version {
                    // resync part
                    ctx.next_state = Some(RemoteStoreState::SyncPart(SyncPartState::new(self.version + 1)));
                }
            }
        }
    }

    fn on_rpc_res(&mut self, _ctx: &mut StateCtx<N, V>, _event: RpcRes<V>) {
        // dont process here
    }
}
