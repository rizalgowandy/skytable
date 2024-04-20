/*
 * Created on Sat Apr 20 2024
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

use {
    crate::{
        engine::{
            error::StorageError,
            storage::{
                common::sdss::sdss_r1::rw::TrackedReader,
                v2::raw::{
                    journal::{
                        self,
                        raw::{
                            CommitPreference, JournalInitializer, RawJournalAdapterEvent,
                            RawJournalWriter,
                        },
                        JournalHeuristics, JournalSettings, JournalStats, RawJournalAdapter,
                    },
                    spec::SystemDatabaseV1,
                },
            },
            RuntimeResult,
        },
        util::compiler::TaggedEnum,
    },
    parking_lot::RwLock,
    sky_macros::TaggedEnum,
    std::collections::HashMap,
};

fn jinit(path: &str) -> RuntimeResult<RawJournalWriter<CompactDBAdapter>> {
    journal::create_journal(path)
}

fn jload(
    db: &CompactDB,
    path: &str,
) -> RuntimeResult<(RawJournalWriter<CompactDBAdapter>, JournalStats)> {
    journal::open_journal(path, db, JournalSettings::default())
}

#[derive(Debug)]
pub struct CompactDB {
    data: RwLock<HashMap<String, String>>,
}

impl Default for CompactDB {
    fn default() -> Self {
        Self::new(RwLock::default())
    }
}

impl CompactDB {
    pub fn new(data: RwLock<HashMap<String, String>>) -> Self {
        Self { data }
    }
    pub fn insert(
        &self,
        jrnl: &mut RawJournalWriter<CompactDBAdapter>,
        key: String,
        val: String,
    ) -> RuntimeResult<()> {
        jrnl.commit_event(Insert(&key, &val))?;
        self.data.write().insert(key, val);
        Ok(())
    }
    pub fn update(
        &self,
        jrnl: &mut RawJournalWriter<CompactDBAdapter>,
        key: String,
        val: String,
    ) -> RuntimeResult<()> {
        jrnl.commit_event(Update(&key, &val))?;
        self.data.write().insert(key, val).unwrap();
        Ok(())
    }
    pub fn remove(
        &self,
        jrnl: &mut RawJournalWriter<CompactDBAdapter>,
        key: String,
    ) -> RuntimeResult<()> {
        self.data.write().remove(&key).unwrap();
        jrnl.commit_event(Remove(&key))
    }
}

pub struct CompactDBAdapter;

#[derive(TaggedEnum, Debug, Clone, Copy)]
#[repr(u8)]
pub enum CompactDBEventKind {
    Insert = 0,
    Update = 1,
    Remove = 2,
}

impl RawJournalAdapter for CompactDBAdapter {
    const COMMIT_PREFERENCE: CommitPreference = CommitPreference::Buffered;
    type Spec = SystemDatabaseV1;
    type GlobalState = CompactDB;
    type Context<'a> = ();
    type CommitContext = ();
    type EventMeta = CompactDBEventKind;
    fn initialize(_: &JournalInitializer) -> Self {
        Self
    }
    fn enter_context<'a>(_: &'a mut RawJournalWriter<Self>) -> Self::Context<'a> {
        ()
    }
    fn parse_event_meta(meta: u64) -> Option<Self::EventMeta> {
        CompactDBEventKind::try_from_raw(meta as _)
    }
    fn commit_buffered<E>(&mut self, buf: &mut Vec<u8>, ev: E, _: Self::CommitContext)
    where
        E: RawJournalAdapterEvent<Self>,
    {
        ev.write_buffered(buf, ())
    }
    fn decode_apply<'a>(
        gs: &Self::GlobalState,
        meta: Self::EventMeta,
        file: &mut TrackedReader<Self::Spec>,
        heuristics: &mut JournalHeuristics,
    ) -> RuntimeResult<()> {
        match meta {
            CompactDBEventKind::Insert => read_kv(file, gs),
            CompactDBEventKind::Update => {
                heuristics.report_new_redundant_record();
                read_kv(file, gs)
            }
            CompactDBEventKind::Remove => {
                heuristics.report_new_redundant_record();
                let klen = u64::from_le_bytes(file.read_block()?);
                if file.remaining() >= klen {
                    let mut key = vec![0; klen as usize];
                    file.tracked_read(&mut key)?;
                    if let Ok(key) = String::from_utf8(key) {
                        let _ = gs.data.write().remove(&key);
                        return Ok(());
                    }
                }
                Err(StorageError::RawJournalDecodeEventCorruptedPayload.into())
            }
        }
    }
}

fn read_kv(
    file: &mut TrackedReader<SystemDatabaseV1>,
    gs: &CompactDB,
) -> Result<(), crate::engine::fractal::error::Error> {
    let klen = u64::from_le_bytes(file.read_block()?);
    let vlen = u64::from_le_bytes(file.read_block()?);
    if file.remaining() >= klen + vlen {
        let mut key = vec![0; klen as usize];
        let mut val = vec![0; vlen as usize];
        file.tracked_read(&mut key)?;
        file.tracked_read(&mut val)?;
        if let (Ok(key), Ok(val)) = (String::from_utf8(key), String::from_utf8(val)) {
            let _ = gs.data.write().insert(key, val);
            return Ok(());
        }
    }
    Err(StorageError::RawJournalDecodeEventCorruptedPayload.into())
}

pub struct Insert<'a>(&'a str, &'a str);
impl<'a> RawJournalAdapterEvent<CompactDBAdapter> for Insert<'a> {
    fn md(&self) -> u64 {
        CompactDBEventKind::Insert.dscr_u64()
    }
    fn write_buffered<'b>(
        self,
        buf: &mut Vec<u8>,
        _: <CompactDBAdapter as RawJournalAdapter>::CommitContext,
    ) {
        buf.extend(&self.0.len().to_le_bytes());
        buf.extend(&self.1.len().to_le_bytes());
        buf.extend(self.0.as_bytes());
        buf.extend(self.1.as_bytes());
    }
}

pub struct Update<'a>(&'a str, &'a str);
impl<'a> RawJournalAdapterEvent<CompactDBAdapter> for Update<'a> {
    fn md(&self) -> u64 {
        CompactDBEventKind::Update.dscr_u64()
    }
    fn write_buffered<'b>(
        self,
        buf: &mut Vec<u8>,
        _: <CompactDBAdapter as RawJournalAdapter>::CommitContext,
    ) {
        buf.extend(&self.0.len().to_le_bytes());
        buf.extend(&self.1.len().to_le_bytes());
        buf.extend(self.0.as_bytes());
        buf.extend(self.1.as_bytes());
    }
}

pub struct Remove<'a>(&'a str);
impl<'a> RawJournalAdapterEvent<CompactDBAdapter> for Remove<'a> {
    fn md(&self) -> u64 {
        CompactDBEventKind::Remove.dscr_u64()
    }
    fn write_buffered<'b>(
        self,
        buf: &mut Vec<u8>,
        _: <CompactDBAdapter as RawJournalAdapter>::CommitContext,
    ) {
        buf.extend(&self.0.len().to_le_bytes());
        buf.extend(self.0.as_bytes());
    }
}

fn genkv(i: usize) -> (String, String) {
    (
        format!("key-{:0>width$}", i, width = 100),
        format!("val-{:0>width$}", i, width = 100),
    )
}

#[test]
fn server_events_only() {
    let mut jrnl = jinit("server_events_only_compact").unwrap();
    RawJournalWriter::close_driver(&mut jrnl).unwrap();
    drop(jrnl); // net drv: 1
    let (mut jrnl, stat) = jload(&CompactDB::default(), "server_events_only_compact").unwrap();
    RawJournalWriter::close_driver(&mut jrnl).unwrap();
    drop(jrnl); // net drv: 1 + 2 = 3
    assert!(!stat.compaction_recommended());
    // we need to create 4 more cycles
    for _ in 0..4 {
        let (mut jrnl, stat) = jload(&CompactDB::default(), "server_events_only_compact").unwrap();
        assert!(!stat.compaction_recommended());
        RawJournalWriter::close_driver(&mut jrnl).unwrap();
    }
    let (_, stat) = jload(&CompactDB::default(), "server_events_only_compact").unwrap();
    assert!(stat.compaction_recommended());
}

#[test]
fn do_not_compact_unique() {
    /*
        we create multiple unique events leading to not redundancy
    */
    let mut jrnl = jinit("do_not_compact_unique").unwrap();
    let cdb = CompactDB::default();
    for (k, v) in (0..100).into_iter().map(genkv) {
        cdb.insert(&mut jrnl, k, v).unwrap();
    }
    RawJournalWriter::close_driver(&mut jrnl).unwrap();
    drop(jrnl);
    let (_, stat) = jload(&cdb, "do_not_compact_unique").unwrap();
    assert!(!stat.compaction_recommended());
}

#[test]
fn compact_because_duplicate() {
    /*
        we create multiple overlapping events leading to redundancy
    */
    let mut jrnl = jinit("do_not_compact_unique").unwrap();
    let cdb = CompactDB::default();
    for (i, (k, v)) in (0..=100).into_iter().map(genkv).enumerate() {
        cdb.insert(&mut jrnl, k.clone(), v.clone()).unwrap();
        if i % 10 == 0 {
            cdb.update(&mut jrnl, k, "W".repeat(100)).unwrap();
        } else if i == 99 {
            // just ensure that we breach the 10%
            cdb.remove(&mut jrnl, k).unwrap();
        }
    }
    RawJournalWriter::close_driver(&mut jrnl).unwrap();
    drop(jrnl);
    let (_, stat) = jload(&cdb, "do_not_compact_unique").unwrap();
    assert!(stat.compaction_recommended());
}

#[test]
fn compact_because_server_event_exceeded() {
    let mut jrnl = jinit("compact_because_server_event_exceeded").unwrap();
    let db = CompactDB::default();
    for (k, v) in (0..5).into_iter().map(genkv) {
        db.insert(&mut jrnl, k, v).unwrap();
    }
    RawJournalWriter::close_driver(&mut jrnl).unwrap();
    drop(jrnl); // 1 drv
    for (k, v) in (5..9).into_iter().map(genkv) {
        let db = CompactDB::default();
        let (mut jrnl, stat) = jload(&db, "compact_because_server_event_exceeded").unwrap();
        assert!(!stat.compaction_recommended());
        db.insert(&mut jrnl, k, v).unwrap();
        RawJournalWriter::close_driver(&mut jrnl).unwrap();
        drop(jrnl);
    }
    // net drv events: 1 + 4*2 = 9. net srv events = 5 + 4 = 9
    let (_, stat) = jload(
        &CompactDB::default(),
        "compact_because_server_event_exceeded",
    )
    .unwrap();
    assert!(stat.compaction_recommended());
}
