/*
 * Created on Sun Jan 21 2024
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2024, Sayan Nandan <nandansayan@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
*/

#[cfg(test)]
mod tests;

use {
    crate::{
        engine::{
            error::{ErrorKind, StorageError, TransactionError},
            fractal::{context, error::Error},
            mem::unsafe_apis::memcpy,
            storage::common::{
                checksum::SCrc64,
                interface::fs::{File, FileExt, FileSystem, FileWriteExt},
                sdss::sdss_r1::{
                    rw::{SdssFile, TrackedReader, TrackedWriter},
                    FileSpecV1,
                },
            },
            RuntimeResult,
        },
        util::compiler::TaggedEnum,
    },
    core::fmt,
    std::{io::ErrorKind as IoErrorKind, ops::Range},
};

/*
    loader
*/

/// Create a new journal
pub fn create_journal<J: RawJournalAdapter>(log_path: &str) -> RuntimeResult<RawJournalWriter<J>>
where
    J::Spec: FileSpecV1<EncodeArgs = ()>,
{
    let log = SdssFile::create(log_path)?;
    RawJournalWriter::new(
        JournalInitializer::new(<J::Spec as FileSpecV1>::SIZE as u64, SCrc64::new(), 0, 0),
        log,
    )
}

pub fn read_journal<J: RawJournalAdapter>(
    log_path: &str,
    gs: &J::GlobalState,
    settings: JournalSettings,
) -> RuntimeResult<JournalStats>
where
    J::Spec: FileSpecV1<DecodeArgs = ()>,
{
    let log = SdssFile::<J::Spec>::open_rw(log_path)?;
    RawJournalReader::<J>::scroll(log, gs, settings).map(|x| x.1)
}

/// Open an existing journal
pub fn open_journal<J: RawJournalAdapter>(
    log_path: &str,
    gs: &J::GlobalState,
    settings: JournalSettings,
) -> RuntimeResult<(RawJournalWriter<J>, JournalStats)>
where
    J::Spec: FileSpecV1<DecodeArgs = ()>,
{
    let log = SdssFile::<J::Spec>::open_rw(log_path)?;
    let (initializer, stats, file) = RawJournalReader::<J>::scroll(log, gs, settings)?;
    RawJournalWriter::new(initializer, file).map(|jw| (jw, stats))
}

#[derive(Debug, PartialEq)]
/// The result of a journal repair operation
pub enum RepairResult {
    /// No errors were detected
    NoErrors,
    /// Definitely lost n bytes, but might have lost more
    UnspecifiedLoss(u64),
}

/**
    Attempts to repair the given journal, **in-place** and returns the number of bytes that were definitely lost and could not
    be repaired.

    **WARNING**: Backup before calling this
*/
pub fn repair_journal<J: RawJournalAdapter>(
    log_path: &str,
    gs: &J::GlobalState,
    settings: JournalSettings,
    repair_mode: JournalRepairMode,
) -> RuntimeResult<RepairResult>
where
    J::Spec: FileSpecV1<DecodeArgs = ()>,
{
    let log = SdssFile::<J::Spec>::open_rw(log_path)?;
    RawJournalReader::<J>::repair(log, gs, settings, repair_mode).map(|(lost, ..)| lost)
}

pub fn compact_journal<'a, const LOG: bool, J: RawJournalAdapter>(
    original_journal_path: &str,
    original_journal: RawJournalWriter<J>,
    full_sync_ctx: J::FullSyncCtx<'a>,
) -> RuntimeResult<RawJournalWriter<J>>
where
    <J as RawJournalAdapter>::Spec: FileSpecV1<DecodeArgs = (), EncodeArgs = ()>,
    <<J as RawJournalAdapter>::Spec as FileSpecV1>::Metadata: Clone,
{
    compact_journal_direct::<LOG, J, _>(
        original_journal_path,
        Some(original_journal),
        full_sync_ctx,
        false,
        |compacted_jrnl_state| {
            context::set_dmsg("reopening compacted journal and restoring state");
            RawJournalWriter::<J>::load_using_backup(
                J::initialize(&JournalInitializer::new(
                    compacted_jrnl_state.log_file_cursor,
                    compacted_jrnl_state.log_file_checksum.clone(),
                    compacted_jrnl_state.adapter_txn_id,
                    compacted_jrnl_state.adapter_known_txn_offset,
                )),
                original_journal_path,
                compacted_jrnl_state,
            )
        },
    )
}

pub fn compact_journal_direct<'a, const LOG: bool, J: RawJournalAdapter, T>(
    original_journal_path: &str,
    original_journal: Option<RawJournalWriter<J>>,
    full_sync_ctx: J::FullSyncCtx<'a>,
    close_journal: bool,
    f_reopen: impl Fn(JournalWriterStateBackup<J>) -> RuntimeResult<T>,
) -> RuntimeResult<T>
where
    <J as RawJournalAdapter>::Spec: FileSpecV1<DecodeArgs = (), EncodeArgs = ()>,
    <<J as RawJournalAdapter>::Spec as FileSpecV1>::Metadata: Clone,
{
    /*
        (1) safely close journal currently pointed to
        ---
        we might suffer a memory blowup or whatever and this might cause unsafe cleanup of the journal. hence,
        we want to make sure that the good journal is not touched until we are fully sure of the status
    */
    if let Some(mut original_journal) = original_journal {
        context::set_dmsg("closing current journal");
        iff!(
            LOG,
            info!("compact: safely closing journal {original_journal_path}")
        );
        RawJournalWriter::close_driver(&mut original_journal)?;
        drop(original_journal);
    }
    /*
        (2) create intermediate journal
    */
    let temporary_journal_path = format!("{original_journal_path}-compacted");
    iff!(
        LOG,
        info!(
            "compact: beginning compaction of journal {original_journal_path} into {temporary_journal_path}"
        )
    );
    context::set_dmsg("creating new journal for compaction");
    let mut intermediary_jrnl = create_journal::<J>(&temporary_journal_path)?;
    /*
        (3) sync all optimized records to intermediate
    */
    context::set_dmsg("syncing optimized journal");
    iff!(
        LOG,
        info!("compact: syncing optimized journal into {temporary_journal_path}")
    );
    J::rewrite_full_journal(&mut intermediary_jrnl, full_sync_ctx)?;
    /*
        (4) temporarily close new file descriptor
    */
    context::set_dmsg("temporarily closing descriptor of new compaction target");
    if close_journal {
        context::set_dmsg("closing new journal");
        RawJournalWriter::close_driver(&mut intermediary_jrnl)?;
    }
    let compacted_jrnl_state = intermediary_jrnl.cleanup()?;
    /*
        (5) point to new journal
    */
    context::set_dmsg("pointing {temporary_journal_path} to {original_journal_path}");
    iff!(LOG, info!("compact: updating currently active journal"));
    FileSystem::rename(&temporary_journal_path, original_journal_path)?;
    /*
        (6) reopen
        ---
        resume state, only verify I/O stream position
    */
    let ret = f_reopen(compacted_jrnl_state)?;
    iff!(
        LOG,
        info!("compact: successfully compacted {original_journal_path}")
    );
    Ok(ret)
}

#[derive(Debug)]
pub struct JournalInitializer {
    cursor: u64,
    checksum: SCrc64,
    last_txn_id: u64,
    last_offset: u64,
}

impl JournalInitializer {
    pub fn new(cursor: u64, checksum: SCrc64, txn_id: u64, last_offset: u64) -> Self {
        Self {
            cursor,
            checksum,
            last_txn_id: txn_id,
            last_offset,
        }
    }
    pub fn cursor(&self) -> u64 {
        self.cursor
    }
    pub fn checksum(&self) -> SCrc64 {
        self.checksum.clone()
    }
    pub fn txn_id(&self) -> u64 {
        self.last_txn_id
    }
    pub fn last_txn_id(&self) -> u64 {
        self.txn_id().saturating_sub(1)
    }
    pub fn is_new(&self) -> bool {
        self.last_offset == 0
    }
    pub fn last_offset(&self) -> u64 {
        self.last_offset
    }
}

/*
    tracing
*/

#[cfg(test)]
pub fn debug_get_trace() -> Vec<JournalTraceEvent> {
    local_mut!(TRACE, |t| core::mem::take(t))
}

#[cfg(test)]
pub fn debug_get_offsets() -> std::collections::BTreeMap<u64, u64> {
    local_mut!(OFFSETS, |offsets| core::mem::take(offsets))
}

#[cfg(test)]
pub fn debug_set_offset_tracking(track: bool) {
    local_mut!(TRACE_OFFSETS, |track_| *track_ = track)
}

#[cfg(test)]
pub fn debug_get_first_meta_triplet() -> Option<(u64, u64, u64)> {
    local_mut!(FIRST_TRIPLET, |tr| core::mem::take(tr))
}

#[derive(Debug, PartialEq)]
#[cfg(test)]
pub enum JournalTraceEvent {
    Writer(JournalWriterTraceEvent),
    Reader(JournalReaderTraceEvent),
}

#[cfg(test)]
direct_from! {
    JournalTraceEvent => {
        JournalWriterTraceEvent as Writer,
        JournalReaderTraceEvent as Reader,
    }
}

#[derive(Debug, PartialEq)]
#[cfg(test)]
pub enum JournalReaderTraceEvent {
    Initialized,
    Completed,
    ClosedAndReachedEof,
    ReopenSuccess,
    // event
    LookingForEvent,
    AttemptingEvent(u64),
    DetectedServerEvent,
    ServerEventMetadataParsed,
    ServerEventAppliedSuccess,
    // drv events
    DriverEventExpectingClose,
    DriverEventCompletedBlockRead,
    DriverEventExpectedCloseGotClose,
    DriverEventExpectingReopenBlock,
    DriverEventExpectingReopenGotReopen,
    // errors
    ErrTxnIdMismatch { expected: u64, current: u64 },
    DriverEventInvalidMetadata,
    ErrInvalidReopenMetadata,
    ErrExpectedCloseGotReopen,
}

#[derive(Debug, PartialEq)]
#[cfg(test)]
pub(super) enum JournalWriterTraceEvent {
    Initialized,
    ReinitializeAttempt,
    ReinitializeComplete,
    // server event
    CommitAttemptForEvent(u64),
    CommitServerEventWroteMetadata,
    CommitServerEventAdapterCompleted,
    CommitCommitServerEventSyncCompleted,
    // driver event
    DriverEventAttemptCommit {
        event: DriverEventKind,
        event_id: u64,
        prev_id: u64,
    },
    DriverEventCompleted,
    DriverClosed,
}

#[cfg(test)]
local! {
    static TRACE: Vec<JournalTraceEvent> = Vec::new();
    static OFFSETS: std::collections::BTreeMap<u64, u64> = Default::default();
    static TRACE_OFFSETS: bool = false;
    static FIRST_TRIPLET: Option<(u64, u64, u64)> = None;
}

macro_rules! jtrace_event_offset {
    ($id:expr, $offset:expr) => {
        #[cfg(test)]
        {
            local_ref!(TRACE_OFFSETS, |should_trace| {
                if *should_trace {
                    local_mut!(OFFSETS, |offsets| assert!(offsets
                        .insert($id, $offset)
                        .is_none()))
                }
            })
        }
    };
}

macro_rules! jtrace {
    ($expr:expr) => {
        #[cfg(test)]
        {
            local_mut!(TRACE, |traces| traces.push($expr.into()))
        }
    };
}

macro_rules! jtrace_writer {
    ($var:ident) => { jtrace!(JournalWriterTraceEvent::$var) };
    ($var:ident $($tt:tt)*) => { jtrace!(JournalWriterTraceEvent::$var$($tt)*) };
}

macro_rules! jtrace_reader {
    ($var:ident) => { jtrace!(JournalReaderTraceEvent::$var) };
    ($var:ident $($tt:tt)*) => { jtrace!(JournalReaderTraceEvent::$var$($tt)*) };
}

/*
    impls
*/

pub trait RawJournalAdapterEvent<CA: RawJournalAdapter>: Sized {
    fn md(&self) -> CA::EventMeta;
    fn write_direct(
        self,
        _: &mut TrackedWriter<<CA as RawJournalAdapter>::Spec>,
        _: <CA as RawJournalAdapter>::CommitContext,
    ) -> RuntimeResult<()> {
        unimplemented!()
    }
    fn write_buffered<'a>(self, _: &mut Vec<u8>, _: <CA as RawJournalAdapter>::CommitContext) {
        unimplemented!()
    }
}

/// An adapter defining the low-level structure of a log file
pub trait RawJournalAdapter: Sized {
    /// event size buffer
    const EVENT_SIZE_BUFFER: usize = 128;
    /// Set to true if the journal writer should automatically flush the buffer and fsync after writing an event
    const AUTO_SYNC_ON_EVENT_COMMIT: bool = true;
    /// set the commit preference
    const COMMIT_PREFERENCE: CommitPreference;
    /// the journal's file spec
    type Spec: FileSpecV1;
    /// the global state that is used by this journal
    type GlobalState;
    /// Writer context
    type Context<'a>
    where
        Self: 'a;
    /// any external context to use during commit. can be used by events
    type CommitContext;
    /// a type representing the event kind
    type EventMeta: TaggedEnum;
    /// the context needed for a full sync of the journal into a (possibly) new intermediary journal
    type FullSyncCtx<'a>;
    fn rewrite_full_journal<'a>(
        writer: &mut RawJournalWriter<Self>,
        full_ctx: Self::FullSyncCtx<'a>,
    ) -> RuntimeResult<()>;
    /// initialize this adapter
    fn initialize(j_: &JournalInitializer) -> Self;
    #[allow(dead_code)]
    /// get a write context
    fn enter_context<'a>(adapter: &'a mut RawJournalWriter<Self>) -> Self::Context<'a>;
    /// parse event metadata
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta>;
    /// commit event (direct preference)
    fn commit_direct<E>(
        &mut self,
        _: &mut TrackedWriter<Self::Spec>,
        _: E,
        _: Self::CommitContext,
    ) -> RuntimeResult<()>
    where
        E: RawJournalAdapterEvent<Self>,
    {
        unimplemented!()
    }
    /// commit event (buffered)
    fn commit_buffered<E>(&mut self, _: &mut Vec<u8>, _: E, _: Self::CommitContext)
    where
        E: RawJournalAdapterEvent<Self>,
    {
        unimplemented!()
    }
    /// decode and apply the event
    fn decode_apply<'a>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        file: &mut TrackedReader<Self::Spec>,
        heuristics: &mut JournalHeuristics,
    ) -> RuntimeResult<()>;
}

#[derive(Debug, PartialEq)]
pub enum CommitPreference {
    #[allow(unused)]
    Buffered,
    Direct,
}

#[derive(Debug, PartialEq)]
/*
    A driver event
    ---
    Structured as:
    +------------------+----------+--------------+------------------+-------------------+-----------------+-----------------+
    |   16B: Event ID  | 8B: Meta | 8B: Checksum | 8B: Payload size | 8B: prev checksum | 8B: prev offset | 8B: prev txn id |
    +------------------+----------+--------------+------------------+-------------------+-----------------+-----------------+
*/
struct DriverEvent {
    txn_id: u128,
    event: DriverEventKind,
    checksum: u64,
    payload_len: u64,
    last_checksum: u64,
    last_offset: u64,
    last_txn_id: u64,
}

impl DriverEvent {
    const FULL_EVENT_SIZE: usize = Self::OFFSET_6_LAST_TXN_ID.end - Self::OFFSET_0_TXN_ID.start;
    /// currently fixed to 24B: last checksum + last offset + last txn id
    const PAYLOAD_LEN: u64 = 3;
    const OFFSET_0_TXN_ID: Range<usize> = 0..sizeof!(u128);
    const OFFSET_1_EVENT_KIND: Range<usize> =
        Self::OFFSET_0_TXN_ID.end..Self::OFFSET_0_TXN_ID.end + sizeof!(u64);
    const OFFSET_2_CHECKSUM: Range<usize> =
        Self::OFFSET_1_EVENT_KIND.end..Self::OFFSET_1_EVENT_KIND.end + sizeof!(u64);
    const OFFSET_3_PAYLOAD_LEN: Range<usize> =
        Self::OFFSET_2_CHECKSUM.end..Self::OFFSET_2_CHECKSUM.end + sizeof!(u64);
    const OFFSET_4_LAST_CHECKSUM: Range<usize> =
        Self::OFFSET_3_PAYLOAD_LEN.end..Self::OFFSET_3_PAYLOAD_LEN.end + sizeof!(u64);
    const OFFSET_5_LAST_OFFSET: Range<usize> =
        Self::OFFSET_4_LAST_CHECKSUM.end..Self::OFFSET_4_LAST_CHECKSUM.end + sizeof!(u64);
    const OFFSET_6_LAST_TXN_ID: Range<usize> =
        Self::OFFSET_5_LAST_OFFSET.end..Self::OFFSET_5_LAST_OFFSET.end + sizeof!(u64);
    /// Create a new driver event (checksum auto-computed)
    fn new(
        txn_id: u128,
        driver_event: DriverEventKind,
        last_checksum: u64,
        last_offset: u64,
        last_txn_id: u64,
    ) -> Self {
        let mut checksum = SCrc64::new();
        checksum.update(&Self::PAYLOAD_LEN.to_le_bytes());
        checksum.update(&last_checksum.to_le_bytes());
        checksum.update(&last_offset.to_le_bytes());
        checksum.update(&last_txn_id.to_le_bytes());
        Self::with_checksum(
            txn_id,
            driver_event,
            checksum.finish(),
            last_checksum,
            last_offset,
            last_txn_id,
        )
    }
    /// Create a new driver event with the given checksum
    fn with_checksum(
        txn_id: u128,
        driver_event: DriverEventKind,
        checksum: u64,
        last_checksum: u64,
        last_offset: u64,
        last_txn_id: u64,
    ) -> Self {
        Self {
            txn_id,
            event: driver_event,
            checksum,
            payload_len: Self::PAYLOAD_LEN as u64,
            last_checksum,
            last_offset,
            last_txn_id,
        }
    }
    /// Encode the current driver event
    fn encode_self(&self) -> [u8; 64] {
        Self::encode(
            self.txn_id,
            self.event,
            self.last_checksum,
            self.last_offset,
            self.last_txn_id,
        )
    }
    /// Encode a new driver event
    ///
    /// Notes:
    /// - The payload length is harcoded to 3
    /// - The checksum is automatically computed
    fn encode(
        txn_id: u128,
        driver_event: DriverEventKind,
        last_checksum: u64,
        last_offset: u64,
        last_txn_id: u64,
    ) -> [u8; 64] {
        const _: () = assert!(DriverEvent::OFFSET_6_LAST_TXN_ID.end == 64);
        let mut block = [0; 64];
        block[Self::OFFSET_0_TXN_ID].copy_from_slice(&txn_id.to_le_bytes());
        block[Self::OFFSET_1_EVENT_KIND]
            .copy_from_slice(&(driver_event.value_u8() as u64).to_le_bytes());
        // the below is a part of the payload
        let mut checksum = SCrc64::new();
        block[Self::OFFSET_3_PAYLOAD_LEN].copy_from_slice(&Self::PAYLOAD_LEN.to_le_bytes());
        block[Self::OFFSET_4_LAST_CHECKSUM].copy_from_slice(&last_checksum.to_le_bytes());
        block[Self::OFFSET_5_LAST_OFFSET].copy_from_slice(&last_offset.to_le_bytes());
        block[Self::OFFSET_6_LAST_TXN_ID].copy_from_slice(&last_txn_id.to_le_bytes());
        checksum.update(&block[Self::OFFSET_3_PAYLOAD_LEN.start..Self::OFFSET_6_LAST_TXN_ID.end]);
        // now update the checksum
        block[Self::OFFSET_2_CHECKSUM].copy_from_slice(&checksum.finish().to_le_bytes());
        block
    }
    fn decode(block: [u8; 64]) -> Option<Self> {
        var!(
            let txn_id, driver_event, checksum, payload_len, last_checksum, last_offset, last_txn_id
        );
        unsafe {
            /*
                UNSAFE(@ohsayan): we've ensured that the block size is exactly 64 and we use the offsets
                correctly
            */
            macro_rules! cpblk {
                ($target:path) => {
                    cpblk!($target as u64)
                };
                ($target:path as $ty:ty) => {
                    <$ty>::from_le_bytes(memcpy(&block[$target]))
                };
            }
            txn_id = cpblk!(Self::OFFSET_0_TXN_ID as u128);
            let driver_event_ = cpblk!(Self::OFFSET_1_EVENT_KIND);
            checksum = cpblk!(Self::OFFSET_2_CHECKSUM);
            payload_len = cpblk!(Self::OFFSET_3_PAYLOAD_LEN);
            last_checksum = cpblk!(Self::OFFSET_4_LAST_CHECKSUM);
            last_offset = cpblk!(Self::OFFSET_5_LAST_OFFSET);
            last_txn_id = cpblk!(Self::OFFSET_6_LAST_TXN_ID);
            // now validate checksum
            let mut checksum_ = SCrc64::new();
            checksum_
                .update(&block[Self::OFFSET_3_PAYLOAD_LEN.start..Self::OFFSET_6_LAST_TXN_ID.end]);
            let target_checksum = checksum_.finish();
            let invalid_ev_dscr = driver_event_ > DriverEventKind::MAX_DSCR as u64;
            let invalid_ck = checksum != target_checksum;
            let invalid_pl_size = payload_len != 3;
            if invalid_ev_dscr | invalid_ck | invalid_pl_size {
                return None;
            }
            driver_event = DriverEventKind::from_raw(driver_event_ as u8);
            Some(Self::with_checksum(
                txn_id,
                driver_event,
                checksum,
                last_checksum,
                last_offset,
                last_txn_id,
            ))
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, sky_macros::EnumMethods, sky_macros::TaggedEnum)]
#[repr(u8)]
pub(super) enum DriverEventKind {
    Reopened = 0,
    Closed = 1,
}

/*
    +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    Journal writer implementation
    ---
    Quick notes:
    - This is a low level writer and only handles driver events
    - Checksum verification is only performed for meta events
    - Implementors must handle checksums themselves

    +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/

pub struct JournalWriterStateBackup<J: RawJournalAdapter> {
    log_file_md: <<J as RawJournalAdapter>::Spec as FileSpecV1>::Metadata,
    log_file_cursor: u64,
    log_file_checksum: SCrc64,
    adapter_txn_id: u64,
    adapter_known_txn_id: u64,
    adapter_known_txn_offset: u64,
}

impl<J: RawJournalAdapter> JournalWriterStateBackup<J> {
    fn new(
        log_file_md: <<J as RawJournalAdapter>::Spec as FileSpecV1>::Metadata,
        log_file_cursor: u64,
        log_file_checksum: SCrc64,
        adapter_txn_id: u64,
        adapter_known_txn_id: u64,
        adapter_known_txn_offset: u64,
    ) -> Self {
        Self {
            log_file_md,
            log_file_cursor,
            log_file_checksum,
            adapter_txn_id,
            adapter_known_txn_id,
            adapter_known_txn_offset,
        }
    }
}

/// A low-level journal writer
pub struct RawJournalWriter<J: RawJournalAdapter> {
    j: J,
    log_file: TrackedWriter<<J as RawJournalAdapter>::Spec>,
    txn_id: u64,
    known_txn_id: u64,
    known_txn_offset: u64, // if offset is 0, txn id is unset
}

impl<J: RawJournalAdapter> RawJournalWriter<J> {
    /// _Forget_ this journal, returning information about the journal's state that can be used to restore the current state later
    /// using [`Self::load_using_backup`]
    ///
    /// **☢☢ WARNING ☢☢** The journal is **never closed** when this is called. Only the file descriptor is.
    fn cleanup(mut self) -> RuntimeResult<JournalWriterStateBackup<J>>
    where
        <<J as RawJournalAdapter>::Spec as FileSpecV1>::Metadata: Clone,
    {
        // fsync + verify cursor
        self.log_file.flush_sync()?;
        self.log_file.verify_cursor()?;
        Ok(JournalWriterStateBackup::new(
            self.log_file.get_md().clone(),
            self.log_file.cursor(),
            self.log_file.checksum_state(),
            self.txn_id,
            self.known_txn_id,
            self.known_txn_offset,
        ))
    }
    /// Restore the journal state due after temporary closure of descriptor.
    ///
    /// **☢☢ WARNING ☢☢** The journal is **never reopened** when this is called. Only the file descriptor is.
    fn load_using_backup(
        adapter: J,
        journal_path: &str,
        JournalWriterStateBackup {
            log_file_md,
            log_file_cursor,
            log_file_checksum,
            adapter_txn_id,
            adapter_known_txn_id,
            adapter_known_txn_offset,
        }: JournalWriterStateBackup<J>,
    ) -> RuntimeResult<Self>
    where
        <J as RawJournalAdapter>::Spec: FileSpecV1<DecodeArgs = ()>,
    {
        let mut f = File::open_rw(journal_path)?;
        f.f_seek_start(log_file_cursor)?;
        let mut log_file =
            TrackedWriter::<J::Spec>::new_full(f, log_file_md, log_file_cursor, log_file_checksum);
        log_file.verify_cursor()?;
        Ok(Self {
            j: adapter,
            log_file,
            txn_id: adapter_txn_id,
            known_txn_id: adapter_known_txn_id,
            known_txn_offset: adapter_known_txn_offset,
        })
    }
}

impl<J: RawJournalAdapter + fmt::Debug> fmt::Debug for RawJournalWriter<J>
where
    <J::Spec as FileSpecV1>::Metadata: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawJournalWriter")
            .field("j", &self.j)
            .field("log_file", &self.log_file)
            .field("txn_id", &self.txn_id)
            .field("known_txn_id", &self.known_txn_id)
            .field("known_txn_offset", &self.known_txn_offset)
            .finish()
    }
}

const SERVER_EV_MASK: u64 = 1 << (u64::BITS - 1);

impl<J: RawJournalAdapter> RawJournalWriter<J> {
    /// Initialize a new [`RawJournalWriter`] using a [`JournalInitializer`]
    pub fn new(j_: JournalInitializer, file: SdssFile<J::Spec>) -> RuntimeResult<Self> {
        let mut me = Self {
            log_file: TrackedWriter::with_cursor_and_checksum(file, j_.cursor(), j_.checksum()),
            known_txn_id: j_.last_txn_id(),
            known_txn_offset: j_.last_offset(),
            txn_id: j_.txn_id(),
            j: J::initialize(&j_),
        };
        if j_.is_new() {
            jtrace_writer!(Initialized);
        } else {
            // not a new instance, so we must update the journal with a re-open event
            jtrace_writer!(ReinitializeAttempt);
            Self::reopen_driver(&mut me)?;
            jtrace_writer!(ReinitializeComplete);
        }
        Ok(me)
    }
    pub fn commit_with_ctx<'a, E: RawJournalAdapterEvent<J>>(
        &mut self,
        event: E,
        ctx: J::CommitContext,
    ) -> RuntimeResult<()> {
        self.txn_context(|me, txn_id| {
            jtrace_writer!(CommitAttemptForEvent(txn_id as u64));
            // MSB must be unused; set msb
            debug_assert!(
                event.md().dscr_u64() & SERVER_EV_MASK != 1,
                "MSB must be unset"
            );
            let ev_md = event.md().dscr_u64() | SERVER_EV_MASK;
            // commit event
            let Self { j, log_file, .. } = me;
            match J::COMMIT_PREFERENCE {
                CommitPreference::Buffered => {
                    // explicitly buffer and then directly write to the file (without buffering)
                    let mut buf = Vec::with_capacity(J::EVENT_SIZE_BUFFER);
                    buf.extend(&txn_id.to_le_bytes());
                    buf.extend(&ev_md.to_le_bytes());
                    jtrace_writer!(CommitServerEventWroteMetadata);
                    j.commit_buffered(&mut buf, event, ctx);
                    log_file.tracked_write_through_buffer(&buf)?;
                }
                CommitPreference::Direct => {
                    // use the underlying buffer
                    // these writes won't actually reach disk
                    log_file.tracked_write(&txn_id.to_le_bytes())?;
                    log_file.tracked_write(&ev_md.to_le_bytes())?;
                    jtrace_writer!(CommitServerEventWroteMetadata);
                    // now hand over control to adapter impl
                    J::commit_direct(j, log_file, event, ctx)?;
                }
            }
            jtrace_writer!(CommitServerEventAdapterCompleted);
            if J::AUTO_SYNC_ON_EVENT_COMMIT {
                // should fsync after event
                log_file.flush_sync()?;
                jtrace_writer!(CommitCommitServerEventSyncCompleted);
            }
            Ok(())
        })
    }
    /// Commit a new event to the journal
    ///
    /// This will auto-flush the buffer and sync metadata as soon as the [`RawJournalAdapter::commit`] method returns,
    /// unless otherwise configured.
    pub fn commit_event<'a, E: RawJournalAdapterEvent<J>>(&mut self, event: E) -> RuntimeResult<()>
    where
        J::CommitContext: Default,
    {
        self.commit_with_ctx(event, Default::default())
    }
    /// roll back to the last txn
    /// WARNING: only call on failure
    ///
    /// NB: Idempotency is guaranteed. Will rollback to, and only to the last event
    pub fn __rollback(&mut self) -> RuntimeResult<()> {
        // ensure cursors are in sync, even if out of position
        self.log_file.verify_cursor()?;
        // reverse
        self.log_file.inner_mut(|file| {
            let new_offset = if self.txn_id == 0 {
                debug_assert_eq!(self.known_txn_offset, 0);
                <<J as RawJournalAdapter>::Spec as FileSpecV1>::SIZE as u64
            } else {
                self.known_txn_offset
            };
            file.f_truncate(new_offset)?;
            Ok(new_offset)
        })?;
        // reverse successful, now empty write buffer
        unsafe {
            // UNSAFE(@ohsayan): since the log has been reversed, whatever we failed to write should simply be ignored
            self.log_file.drain_buffer();
        }
        Ok(())
    }
}

impl<J: RawJournalAdapter> RawJournalWriter<J> {
    fn txn_context<T>(
        &mut self,
        f: impl FnOnce(&mut Self, u128) -> RuntimeResult<T>,
    ) -> RuntimeResult<T> {
        #[cfg(test)]
        if local_ref!(FIRST_TRIPLET, |tr| { tr.is_none() }) {
            local_mut!(FIRST_TRIPLET, |tr| {
                *tr = Some((
                    self.known_txn_id,
                    self.known_txn_offset,
                    self.log_file.current_checksum(),
                ));
            })
        }
        let id = self.txn_id;
        let ret = f(self, id as u128);
        if ret.is_ok() {
            jtrace_event_offset!(id, self.log_file.cursor());
            self.known_txn_id = id;
            self.known_txn_offset = self.log_file.cursor();
            self.txn_id += 1;
        }
        ret
    }
    /// Commit a new driver event
    fn _commit_driver_event(me: &mut Self, kind: DriverEventKind) -> RuntimeResult<()> {
        jtrace_writer!(DriverEventAttemptCommit {
            event: kind,
            event_id: me.txn_id,
            prev_id: me.known_txn_id
        });
        me.txn_context(|me, txn_id| {
            let block = DriverEvent::encode(
                txn_id,
                kind,
                me.log_file.current_checksum(),
                me.known_txn_offset,
                me.known_txn_id,
            );
            if !J::AUTO_SYNC_ON_EVENT_COMMIT {
                // the log might still not be fully flushed, so flush it now; NB: flush does not affect checksum state;
                // this is guaranteed by the impl of the tracked writer
                me.log_file.flush_sync()?;
            }
            me.log_file.tracked_write_through_buffer(&block)?;
            jtrace_writer!(DriverEventCompleted);
            Ok(())
        })
    }
    /// Close driver
    pub fn close_driver(me: &mut Self) -> RuntimeResult<()> {
        Self::_commit_driver_event(me, DriverEventKind::Closed)?;
        jtrace_writer!(DriverClosed);
        Ok(())
    }
    /// Reopen driver
    pub fn reopen_driver(me: &mut Self) -> RuntimeResult<()> {
        Self::_commit_driver_event(me, DriverEventKind::Reopened)?;
        Ok(())
    }
}

pub struct RawJournalReader<J: RawJournalAdapter> {
    tr: TrackedReader<<J as RawJournalAdapter>::Spec>,
    txn_id: u64,
    last_txn_id: u64,
    last_txn_offset: u64,
    last_txn_checksum: u64,
    stats: JournalStats,
    _settings: JournalSettings,
    state: JournalState,
}

#[derive(Debug, PartialEq)]
enum JournalState {
    AwaitingEvent,
    AwaitingServerEvent,
    AwaitingClose,
    AwaitingReopen,
}

impl Default for JournalState {
    fn default() -> Self {
        Self::AwaitingEvent
    }
}

#[derive(Debug)]
pub struct JournalSettings {}

impl Default for JournalSettings {
    fn default() -> Self {
        Self::new()
    }
}

impl JournalSettings {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub struct JournalStats {
    header: usize,
    driver_events: usize,
    heuristics: JournalHeuristics,
    file_size: usize,
}

#[derive(Debug)]
pub struct JournalHeuristics {
    server_events: usize,
    redundant_records: usize,
}

impl JournalHeuristics {
    #[inline(always)]
    pub fn report_additional_redundant_records(&mut self, additional: usize) {
        self.redundant_records += additional;
    }
    #[inline(always)]
    pub fn report_new_redundant_record(&mut self) {
        self.report_additional_redundant_records(1)
    }
    pub fn increment_server_event_count(&mut self) {
        self.server_events += 1;
    }
    #[cfg(test)]
    pub fn get_current_redundant(&self) -> usize {
        self.redundant_records
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Recommendation {
    NoActionNeeded,
    CompactDrvHighRatio,
    CompactRedHighRatio,
}

impl Recommendation {
    pub const fn needs_compaction(&self) -> bool {
        matches!(self, Self::CompactDrvHighRatio | Self::CompactRedHighRatio)
    }
    pub const fn reason_str(&self) -> &'static str {
        match self {
            Self::NoActionNeeded => "no action needed",
            Self::CompactDrvHighRatio => "drv_high_ratio",
            Self::CompactRedHighRatio => "srv_high_redundancy",
        }
    }
}

impl JournalStats {
    /// Returns true if a compaction would be prudent
    pub fn recommended_action(&self) -> Recommendation {
        let minimum_file_size_compaction_trigger: usize = if cfg!(test) {
            (DriverEvent::FULL_EVENT_SIZE * 10) + self.header
        } else {
            4 * 1024 * 1024
        };
        let total_records = self.heuristics.server_events + self.driver_events;
        let server_event_percentage =
            (self.heuristics.server_events as f64 / total_records as f64) * 100.0;
        let driver_event_percentage = (self.driver_events as f64 / total_records as f64) * 100.0;
        let redundant_record_percentage = if self.heuristics.server_events == 0 {
            0.0
        } else {
            (self.heuristics.redundant_records as f64 / self.heuristics.server_events as f64)
                * 100.00
        };
        if self.file_size >= minimum_file_size_compaction_trigger {
            if driver_event_percentage >= server_event_percentage {
                return Recommendation::CompactDrvHighRatio;
            }
            if redundant_record_percentage >= 10.0 {
                return Recommendation::CompactRedHighRatio;
            }
        }
        Recommendation::NoActionNeeded
    }
    fn new<J: RawJournalAdapter>() -> Self {
        Self {
            driver_events: 0,
            heuristics: JournalHeuristics {
                server_events: 0,
                redundant_records: 0,
            },
            file_size: 0,
            header: <<J as RawJournalAdapter>::Spec as FileSpecV1>::SIZE,
        }
    }
    fn set_file_size(&mut self, size: usize) {
        self.file_size = size;
    }
}

impl<J: RawJournalAdapter> RawJournalReader<J> {
    fn scroll(
        file: SdssFile<<J as RawJournalAdapter>::Spec>,
        gs: &J::GlobalState,
        settings: JournalSettings,
    ) -> RuntimeResult<(JournalInitializer, JournalStats, SdssFile<J::Spec>)> {
        let reader = TrackedReader::with_cursor(
            file,
            <<J as RawJournalAdapter>::Spec as FileSpecV1>::SIZE as u64,
        )?;
        jtrace_reader!(Initialized);
        let mut me = Self::new(reader, 0, 0, 0, 0, settings);
        me.stats.set_file_size(me.tr.cached_size() as usize);
        me._scroll(gs)
            .map(|jinit| (jinit, me.stats, me.tr.into_inner()))
    }
    fn _scroll(&mut self, gs: &J::GlobalState) -> RuntimeResult<JournalInitializer> {
        loop {
            jtrace_reader!(LookingForEvent);
            match self._apply_next_event_and_stop(gs) {
                Ok(true) => {
                    jtrace_reader!(Completed);
                    let initializer = JournalInitializer::new(
                        self.tr.cursor(),
                        self.tr.checksum(),
                        self.txn_id,
                        // NB: the last txn offset is important because it indicates that the log is new
                        self.last_txn_offset,
                    );
                    return Ok(initializer);
                }
                Ok(false) => self.state = JournalState::AwaitingEvent,
                Err(e) => return Err(e),
            }
        }
    }
    fn new(
        reader: TrackedReader<<J as RawJournalAdapter>::Spec>,
        txn_id: u64,
        last_txn_id: u64,
        last_txn_offset: u64,
        last_txn_checksum: u64,
        settings: JournalSettings,
    ) -> Self {
        Self {
            tr: reader,
            txn_id,
            last_txn_id,
            last_txn_offset,
            last_txn_checksum,
            stats: JournalStats::new::<J>(),
            _settings: settings,
            state: JournalState::AwaitingEvent,
        }
    }
    fn __refresh_known_txn(me: &mut Self) {
        me.last_txn_id = me.txn_id;
        me.last_txn_checksum = me.tr.current_checksum();
        me.last_txn_offset = me.tr.cursor();
        me.txn_id += 1;
    }
}

#[derive(Debug, PartialEq)]
pub enum JournalRepairMode {
    Simple,
}

impl<J: RawJournalAdapter> RawJournalReader<J> {
    fn repair(
        file: SdssFile<<J as RawJournalAdapter>::Spec>,
        gs: &J::GlobalState,
        settings: JournalSettings,
        repair_mode: JournalRepairMode,
    ) -> RuntimeResult<(RepairResult, JournalInitializer, SdssFile<J::Spec>)> {
        let reader = TrackedReader::with_cursor(
            file,
            <<J as RawJournalAdapter>::Spec as FileSpecV1>::SIZE as u64,
        )?;
        jtrace_reader!(Initialized);
        let mut me = Self::new(reader, 0, 0, 0, 0, settings);
        match me._scroll(gs) {
            Ok(init) => return Ok((RepairResult::NoErrors, init, me.tr.into_inner())),
            Err(e) => me.start_repair(e, repair_mode),
        }
    }
    fn start_repair(
        self,
        e: Error,
        repair_mode: JournalRepairMode,
    ) -> RuntimeResult<(RepairResult, JournalInitializer, SdssFile<J::Spec>)> {
        let lost = if self.last_txn_offset == 0 {
            // we haven't scanned any events and already hit an error
            // so essentially, we lost the entire log
            self.tr.cached_size() - <J::Spec as FileSpecV1>::SIZE as u64
        } else {
            self.tr.cached_size() - self.last_txn_offset
        };
        let repair_result = RepairResult::UnspecifiedLoss(lost);
        match repair_mode {
            JournalRepairMode::Simple => {}
        }
        // now it's our task to determine exactly what happened
        match e.kind() {
            ErrorKind::IoError(io) => match io.kind() {
                IoErrorKind::UnexpectedEof => {
                    /*
                        this is the only kind of error that we can actually repair since it indicates that a part of the
                        file is "missing." we can't deal with things like permission errors. that's supposed to be handled
                        by the admin by looking through the error logs
                    */
                }
                _ => return Err(e),
            },
            ErrorKind::Storage(e) => match e {
                // unreachable errors (no execution path here)
                StorageError::RuntimeUpgradeFailureFileIsNewer
                | StorageError::RawJournalRuntimeDirty
                | StorageError::RuntimeEngineLoadError
                | StorageError::RuntimeRestoreValidationFailure
                | StorageError::FileDecodeHeaderVersionMismatch         // should be caught earlier
                | StorageError::FileDecodeHeaderCorrupted               // should be caught earlier
                | StorageError::V1JournalDecodeLogEntryCorrupted        // v1 errors can't be raised here
                | StorageError::V1JournalDecodeCorrupted
                | StorageError::V1DataBatchDecodeCorruptedBatch
                | StorageError::V1DataBatchDecodeCorruptedEntry
                | StorageError::V1DataBatchDecodeCorruptedBatchFile
                | StorageError::V1SysDBDecodeCorrupted
                | StorageError::V1DataBatchRuntimeCloseError => unreachable!(),
                // possible errors
                StorageError::InternalDecodeStructureCorrupted
                | StorageError::InternalDecodeStructureCorruptedPayload
                | StorageError::InternalDecodeStructureIllegalData
                | StorageError::RawJournalDecodeEventCorruptedMetadata
                | StorageError::RawJournalDecodeEventCorruptedPayload
                | StorageError::RawJournalDecodeBatchContentsMismatch
                | StorageError::RawJournalDecodeBatchIntegrityFailure
                | StorageError::RawJournalDecodeInvalidEvent
                | StorageError::RawJournalDecodeCorruptionInBatchMetadata => {}
            },
            ErrorKind::Txn(txerr) => match txerr {
                // unreachable errors
                TransactionError::V1DecodeCorruptedPayloadMoreBytes                 // no v1 errors
                | TransactionError::V1DecodedUnexpectedEof
                | TransactionError::V1DecodeUnknownTxnOp => unreachable!(),
                // possible errors
                TransactionError::OnRestoreDataConflictAlreadyExists |
                TransactionError::OnRestoreDataMissing |
                TransactionError::OnRestoreDataConflictMismatch => {},
            },
            // these errors do not have an execution pathway
            ErrorKind::Other(_) => unreachable!(),
            ErrorKind::Config(_) => unreachable!(),
        }
        /*
            revert log. record previous signatures.
        */
        l!(let known_event_id, known_event_offset, known_event_checksum = self.last_txn_id, self.last_txn_offset, self.last_txn_checksum);
        let mut last_logged_checksum = self.tr.checksum();
        let mut base_log = self.tr.into_inner();
        if known_event_offset == 0 {
            // no event, so just trim upto header
            base_log.truncate(<J::Spec as FileSpecV1>::SIZE as _)?;
        } else {
            base_log.truncate(known_event_offset)?;
        }
        /*
            see what needs to be done next
        */
        match self.state {
            JournalState::AwaitingEvent
            | JournalState::AwaitingServerEvent
            | JournalState::AwaitingClose => {
                /*
                    no matter what the last event was (and definitely not a close since if we are expecting a close the log was not already closed),
                    the log is in a dirty state that can only be resolved by closing it
                */
                let drv_close = DriverEvent::new(
                    if known_event_offset == 0 {
                        // no event occurred
                        0
                    } else {
                        // something happened prior to this, so we'll use an incremented ID for this event
                        known_event_id + 1
                    } as u128,
                    DriverEventKind::Closed,
                    known_event_checksum,
                    known_event_offset,
                    known_event_id,
                );
                let drv_close_event = drv_close.encode_self();
                last_logged_checksum.update(&drv_close_event);
                base_log.fsynced_write(&drv_close_event)?;
            }
            JournalState::AwaitingReopen => {
                // extra bytes indicating low to severe corruption; last event is a close, so with the revert the log is now clean
            }
        }
        let jinit_cursor = known_event_offset + DriverEvent::FULL_EVENT_SIZE as u64;
        let jinit_last_txn_offset = jinit_cursor; // same as cursor
        let jinit_event_id = known_event_id + 2; // since we already used +1
        let jinit_checksum = last_logged_checksum;
        Ok((
            repair_result,
            JournalInitializer::new(
                jinit_cursor,
                jinit_checksum,
                jinit_event_id,
                jinit_last_txn_offset,
            ),
            base_log,
        ))
    }
}

impl<J: RawJournalAdapter> RawJournalReader<J> {
    fn _apply_next_event_and_stop(&mut self, gs: &J::GlobalState) -> RuntimeResult<bool> {
        let txn_id = u128::from_le_bytes(self.tr.read_block()?);
        let meta = u64::from_le_bytes(self.tr.read_block()?);
        if txn_id != self.txn_id as u128 {
            jtrace_reader!(ErrTxnIdMismatch {
                expected: self.txn_id,
                current: txn_id as u64
            });
            return Err(StorageError::RawJournalDecodeEventCorruptedMetadata.into());
        }
        jtrace_reader!(AttemptingEvent(txn_id as u64));
        // check for a server event
        // is this a server event?
        if meta & SERVER_EV_MASK != 0 {
            self.state = JournalState::AwaitingServerEvent;
            jtrace_reader!(DetectedServerEvent);
            let meta = meta & !SERVER_EV_MASK;
            match J::parse_event_meta(meta) {
                Some(meta) => {
                    jtrace_reader!(ServerEventMetadataParsed);
                    // now parse the actual event
                    let Self { tr: reader, .. } = self;
                    // we do not consider a parsed event a success signal; so we must actually apply it
                    match J::decode_apply(gs, meta, reader, &mut self.stats.heuristics) {
                        Ok(()) => {
                            jtrace_reader!(ServerEventAppliedSuccess);
                            Self::__refresh_known_txn(self);
                            return Ok(false);
                        }
                        Err(e) => return Err(e),
                    }
                }
                None => return Err(StorageError::RawJournalDecodeEventCorruptedMetadata.into()),
            }
        }
        self.state = JournalState::AwaitingClose;
        return self.handle_close(txn_id, meta);
    }
    fn handle_close(
        &mut self,
        txn_id: u128,
        meta: u64,
    ) -> Result<bool, crate::engine::fractal::error::Error> {
        jtrace_reader!(DriverEventExpectingClose);
        // attempt to parse a driver close event
        let mut block = [0u8; DriverEvent::FULL_EVENT_SIZE];
        block[DriverEvent::OFFSET_0_TXN_ID].copy_from_slice(&txn_id.to_le_bytes());
        block[DriverEvent::OFFSET_1_EVENT_KIND].copy_from_slice(&meta.to_le_bytes());
        // now get remaining block
        self.tr
            .tracked_read(&mut block[DriverEvent::OFFSET_2_CHECKSUM.start..])?;
        jtrace_reader!(DriverEventCompletedBlockRead);
        // check the driver event
        let drv_close_event = match DriverEvent::decode(block) {
            Some(
                ev @ DriverEvent {
                    event: DriverEventKind::Closed,
                    ..
                },
            ) => ev,
            Some(DriverEvent {
                event: DriverEventKind::Reopened,
                ..
            }) => {
                jtrace_reader!(ErrExpectedCloseGotReopen);
                return Err(StorageError::RawJournalDecodeInvalidEvent.into());
            }
            None => return Err(StorageError::RawJournalDecodeEventCorruptedPayload.into()),
        };
        jtrace_reader!(DriverEventExpectedCloseGotClose);
        // a driver closed event; we've checked integrity, but we must check the field values
        let valid_meta = okay! {
            /*
                basically:
                - if this is a new journal all these values are 0 (we're essentially reading the first event)
                - otherwise, it is the last event offset
            */
            self.last_txn_checksum == drv_close_event.last_checksum,
            self.last_txn_id == drv_close_event.last_txn_id,
            self.last_txn_offset == drv_close_event.last_offset,
        };
        if !valid_meta {
            jtrace_reader!(DriverEventInvalidMetadata);
            // either the block is corrupted or the data we read is corrupted; either way,
            // we're going to refuse to read this
            return Err(StorageError::RawJournalDecodeEventCorruptedMetadata.into());
        }
        self.stats.driver_events += 1;
        // update
        Self::__refresh_known_txn(self);
        // full metadata validated; this is a valid close event, but is it actually a close?
        if self.tr.is_eof() {
            jtrace_reader!(ClosedAndReachedEof);
            // yes, we're done
            return Ok(true);
        }
        self.state = JournalState::AwaitingReopen;
        jtrace_reader!(DriverEventExpectingReopenBlock);
        return self.handle_reopen();
    }
    fn handle_reopen(&mut self) -> RuntimeResult<bool> {
        jtrace_reader!(AttemptingEvent(self.txn_id as u64));
        // now we must look for a reopen event
        let event_block = self.tr.read_block::<{ DriverEvent::FULL_EVENT_SIZE }>()?;
        let reopen_event = match DriverEvent::decode(event_block) {
            Some(ev) if ev.event == DriverEventKind::Reopened => ev,
            Some(_) => return Err(StorageError::RawJournalDecodeInvalidEvent.into()),
            None => return Err(StorageError::RawJournalDecodeEventCorruptedPayload.into()),
        };
        jtrace_reader!(DriverEventExpectingReopenGotReopen);
        let valid_meta = okay! {
            self.last_txn_checksum == reopen_event.last_checksum,
            self.last_txn_id == reopen_event.last_txn_id,
            self.last_txn_offset == reopen_event.last_offset,
            self.txn_id as u128 == reopen_event.txn_id,
        };
        if valid_meta {
            // valid meta, update all
            Self::__refresh_known_txn(self);
            self.stats.driver_events += 1;
            jtrace_reader!(ReopenSuccess);
            Ok(false)
        } else {
            jtrace_reader!(ErrInvalidReopenMetadata);
            Err(StorageError::RawJournalDecodeEventCorruptedMetadata.into())
        }
    }
}
