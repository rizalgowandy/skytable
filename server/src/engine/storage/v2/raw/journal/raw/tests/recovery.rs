/*
 * Created on Tue Mar 26 2024
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
    super::{SimpleDB, SimpleDBJournal},
    crate::{
        engine::{
            error::ErrorKind,
            storage::{
                common::{
                    interface::fs::{File, FileExt, FileSystem, FileWrite, FileWriteExt},
                    sdss::sdss_r1::FileSpecV1,
                },
                v2::raw::journal::{
                    create_journal, open_journal,
                    raw::{
                        debug_get_offsets, debug_get_trace, debug_set_offset_tracking, DriverEvent,
                        DriverEventKind, JournalReaderTraceEvent, JournalWriterTraceEvent,
                        RawJournalWriter,
                    },
                    repair_journal, JournalRepairMode, JournalSettings, RawJournalAdapter,
                    RepairResult,
                },
            },
            RuntimeResult,
        },
        IoResult,
    },
    std::{collections::BTreeMap, io::ErrorKind as IoErrorKind, ops::Range},
};

const TRIALS: usize = 100;

struct Initializer {
    journal_id: &'static str,
    initializer_fn: fn(&str) -> RuntimeResult<u64>,
    last_event_size: usize,
}

impl Initializer {
    fn new(name: &'static str, f: fn(&str) -> RuntimeResult<u64>, last_event_size: usize) -> Self {
        Self {
            journal_id: name,
            initializer_fn: f,
            last_event_size,
        }
    }
    fn new_driver_type(name: &'static str, f: fn(&str) -> RuntimeResult<u64>) -> Self {
        Self::new(name, f, DriverEvent::FULL_EVENT_SIZE)
    }
}

#[derive(Debug)]
#[allow(unused)]
struct ModifiedJournalInfo {
    original_file_size: usize,
    modified_file_size: usize,
    corruption_range: Range<usize>,
}

impl ModifiedJournalInfo {
    fn new(
        original_file_size: usize,
        modified_file_size: usize,
        corruption_range: Range<usize>,
    ) -> Self {
        Self {
            original_file_size,
            modified_file_size,
            corruption_range,
        }
    }
}

fn emulate_sequentially_varying_single_corruption(
    initializers: impl IntoIterator<Item = Initializer>,
    modified_journal_generator_fn: impl Fn(
        &str,
        &str,
        u64,
        usize,
        &BTreeMap<u64, u64>,
    ) -> IoResult<ModifiedJournalInfo>,
    post_corruption_handler: impl Fn(
        &str,
        u64,
        usize,
        SimpleDB,
        RuntimeResult<RawJournalWriter<SimpleDBJournal>>,
        ModifiedJournalInfo,
    ),
    post_repair_handler: impl Fn(
        &str,
        u64,
        usize,
        RuntimeResult<RepairResult>,
        SimpleDB,
        RuntimeResult<RawJournalWriter<SimpleDBJournal>>,
    ),
) {
    for Initializer {
        journal_id,
        initializer_fn,
        last_event_size,
    } in initializers
    {
        // initialize journal, get size and clear traces
        let repaired_last_event_id = match initializer_fn(journal_id) {
            Ok(nid) => nid,
            Err(e) => panic!(
                "failed to initialize {journal_id} due to {e}. trace: {:?}, file_data={:?}",
                debug_get_trace(),
                FileSystem::read(journal_id),
            ),
        };
        let _ = debug_get_trace();
        let original_offsets = debug_get_offsets();
        // now trim and repeat
        for trim_size in 1..=last_event_size {
            // create a copy of the "good" journal and corrupt it
            let corrupted_journal_path = format!("{journal_id}-trimmed-{trim_size}.db");
            let open_journal_fn = |db: &SimpleDB| {
                open_journal::<SimpleDBJournal>(
                    &corrupted_journal_path,
                    db,
                    JournalSettings::default(),
                )
            };
            // modify journal
            let mod_stat = modified_journal_generator_fn(
                journal_id,
                &corrupted_journal_path,
                repaired_last_event_id,
                trim_size,
                &original_offsets,
            )
            .unwrap();
            // now let the caller handle any post corruption work
            {
                let sdb = SimpleDB::new();
                let open_journal_result = open_journal_fn(&sdb);
                post_corruption_handler(
                    journal_id,
                    repaired_last_event_id,
                    trim_size,
                    sdb,
                    open_journal_result,
                    mod_stat,
                );
            }
            // repair and let the caller handle post repair work
            {
                let sdb = SimpleDB::new();
                let repair_result = repair_journal::<SimpleDBJournal>(
                    &corrupted_journal_path,
                    &sdb,
                    JournalSettings::default(),
                    JournalRepairMode::Simple,
                );
                let repaired_journal_reopen_result = open_journal_fn(&sdb);
                // let caller handle any post repair work
                post_repair_handler(
                    journal_id,
                    repaired_last_event_id,
                    trim_size,
                    repair_result,
                    sdb,
                    repaired_journal_reopen_result,
                );
            }
        }
    }
}

fn emulate_final_event_corruption(
    initializers: impl IntoIterator<Item = Initializer>,
    post_corruption_handler: impl Fn(
        &str,
        u64,
        usize,
        SimpleDB,
        RuntimeResult<RawJournalWriter<SimpleDBJournal>>,
        ModifiedJournalInfo,
    ),
    post_repair_handler: impl Fn(
        &str,
        u64,
        usize,
        RuntimeResult<RepairResult>,
        SimpleDB,
        RuntimeResult<RawJournalWriter<SimpleDBJournal>>,
    ),
) {
    emulate_sequentially_varying_single_corruption(
        initializers,
        |original_journal, modified_journal, _, trim_amount, _offsets| {
            FileSystem::copy(original_journal, modified_journal)?;
            let mut f = File::open(modified_journal)?;
            let real_flen = f.f_len()? as usize;
            f.f_truncate((real_flen - trim_amount) as _)?;
            Ok(ModifiedJournalInfo::new(
                real_flen,
                trim_amount,
                trim_amount..real_flen,
            ))
        },
        post_corruption_handler,
        post_repair_handler,
    )
}

fn emulate_midway_corruption(
    initializers: impl IntoIterator<Item = Initializer>,
    post_corruption_handler: impl Fn(
        &str,
        u64,
        usize,
        SimpleDB,
        RuntimeResult<RawJournalWriter<SimpleDBJournal>>,
        ModifiedJournalInfo,
    ),
    post_repair_handler: impl Fn(
        &str,
        u64,
        usize,
        RuntimeResult<RepairResult>,
        SimpleDB,
        RuntimeResult<RawJournalWriter<SimpleDBJournal>>,
    ),
) {
    emulate_sequentially_varying_single_corruption(
        initializers,
        |original_journal_path,
         corrupted_journal_path,
         event_to_corrupt,
         trim_size,
         original_offsets| {
            let orig_journal_data = FileSystem::read(original_journal_path)?;
            let orig_journal_size = orig_journal_data.len();
            let mut f = File::create(corrupted_journal_path)?;
            let end_offset = *original_offsets.get(&event_to_corrupt).unwrap() as usize;
            // apply
            let segment_before_corruption = &orig_journal_data[..end_offset - trim_size];
            let segment_after_corruption = &orig_journal_data[end_offset..];
            let new_size = segment_before_corruption.len() + segment_after_corruption.len();
            assert!(
                new_size < orig_journal_size,
                "real len is {orig_journal_size} while new len is {new_size}",
            );
            assert_eq!(
                segment_before_corruption.len() + segment_after_corruption.len() + trim_size,
                orig_journal_size
            );
            f.fwrite_all(segment_before_corruption)?;
            f.fwrite_all(segment_after_corruption)?;
            Ok(ModifiedJournalInfo::new(
                orig_journal_size,
                new_size,
                end_offset - trim_size..end_offset,
            ))
        },
        post_corruption_handler,
        post_repair_handler,
    )
}

fn apply_event_mix(jrnl: &mut RawJournalWriter<SimpleDBJournal>) -> RuntimeResult<u64> {
    let mut op_count = 0;
    let mut sdb = SimpleDB::new();
    for num in 1..=TRIALS {
        op_count += 1;
        sdb.push(jrnl, format!("key-{num:06}"))?;
        if num % 10 == 0 {
            op_count += 1;
            sdb.pop(jrnl)?;
        }
    }
    Ok(op_count)
}

#[test]
fn corruption_before_close() {
    let initializers = vec![
        // open and close
        Initializer::new_driver_type("close_event_corruption_empty.db", |jrnl_id| {
            let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
            RawJournalWriter::close_driver(&mut jrnl)?;
            Ok(0)
        }),
        // open, apply mix of events, close
        Initializer::new_driver_type("close_event_corruption.db", |jrnl_id| {
            let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
            let operation_count = apply_event_mix(&mut jrnl)?;
            RawJournalWriter::close_driver(&mut jrnl)?;
            Ok(operation_count)
        }),
        // open, close, reinit, close
        Initializer::new_driver_type(
            "close_event_corruption_open_close_open_close.db",
            |jrnl_id| {
                // open and close
                let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
                RawJournalWriter::close_driver(&mut jrnl)?;
                drop(jrnl);
                // reinit and close
                let mut jrnl = open_journal::<SimpleDBJournal>(
                    jrnl_id,
                    &SimpleDB::new(),
                    JournalSettings::default(),
                )?;
                RawJournalWriter::close_driver(&mut jrnl)?;
                Ok(2)
            },
        ),
    ];
    emulate_final_event_corruption(
        initializers,
        |journal_id, repaired_last_event_id, trim_size, _db, open_result, _modstat| {
            // open the journal and validate failure
            let open_err = open_result.unwrap_err();
            let trace = debug_get_trace();
            if trim_size > (DriverEvent::FULL_EVENT_SIZE - (sizeof!(u128) + sizeof!(u64))) {
                // the amount of trim from the end of the file causes us to lose valuable metadata
                if repaired_last_event_id == 0 {
                    // empty log
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent
                        ],
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    )
                } else {
                    assert_eq!(
                        *trace.last().unwrap(),
                        JournalReaderTraceEvent::LookingForEvent.into(),
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    );
                }
            } else {
                // the amount of trim still allows us to read some metadata
                if repaired_last_event_id == 0 {
                    // empty log
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id),
                            JournalReaderTraceEvent::DriverEventExpectingClose,
                        ],
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    )
                } else {
                    assert_eq!(
                        &trace[trace.len() - 3..],
                        &into_array![
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id),
                            JournalReaderTraceEvent::DriverEventExpectingClose
                        ],
                        "failed at trim_size {trim_size} for journal {journal_id}"
                    );
                }
            }
            assert_eq!(
                open_err.kind(),
                &ErrorKind::IoError(IoErrorKind::UnexpectedEof.into()),
                "failed at trim_size {trim_size} for journal {journal_id}"
            );
        },
        |journal_id, _repaired_last_id, trim_size, repair_result, _db, reopen_result| {
            assert_eq!(
                repair_result.unwrap(),
                RepairResult::UnspecifiedLoss((DriverEvent::FULL_EVENT_SIZE - trim_size) as _),
                "failed at trim_size {trim_size} for journal {journal_id}"
            );
            let _ = reopen_result.unwrap();
            // clear trace
            let _ = debug_get_trace();
            let _ = debug_get_offsets();
        },
    )
}

#[test]
fn corruption_after_reopen() {
    let initializers = vec![
        Initializer::new_driver_type("corruption_after_reopen.db", |jrnl_id| {
            let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
            RawJournalWriter::close_driver(&mut jrnl)?;
            drop(jrnl);
            // reopen, but don't close
            open_journal::<SimpleDBJournal>(jrnl_id, &SimpleDB::new(), JournalSettings::default())?;
            Ok(1)
        }),
        Initializer::new_driver_type("corruption_after_ropen_multi_before_close.db", |jrnl_id| {
            let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
            let operation_count = apply_event_mix(&mut jrnl)?;
            RawJournalWriter::close_driver(&mut jrnl)?;
            drop(jrnl);
            // reopen, but don't close
            open_journal::<SimpleDBJournal>(jrnl_id, &SimpleDB::new(), JournalSettings::default())?;
            Ok(operation_count + 1) // + 1 since we have the reopen event which is the next event that'll vanish
        }),
    ];
    emulate_final_event_corruption(
        initializers,
        |journal_id, repaired_last_event_id, trim_size, _db, open_result, _modstat| {
            let trace = debug_get_trace();
            if trim_size == DriverEvent::FULL_EVENT_SIZE {
                /*
                    IMPORTANT IFFY SITUATION: undetectable error. if an entire "correct" part of the log vanishes, it's not going to be detected.
                    while possible in theory, it's going to have to be one heck of a coincidence for it to happen in practice. the only way to work
                    around this is to use a secondary checksum. I'm not a fan of that approach either (and I don't even consider it to be a good mitigation)
                    because it can potentially violate consistency, conflicting the source of truth. for example: if we have a database crash, should we trust
                    the checksum file or the log? guarding that further requires an enormous amount of effort and it will still have holes and ironically,
                    will potentially introduce more bugs due to increased complexity. Get a good filesystem and disk controller (that attaches checksums to sectors)!
                    -- @ohsayan
                */
                let mut jrnl =
                    open_result.expect(&format!("failed at {trim_size} for journal {journal_id}"));
                if repaired_last_event_id == 1 {
                    // empty log, only the reopen
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(0),
                            JournalReaderTraceEvent::DriverEventExpectingClose,
                            JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                            JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                            JournalReaderTraceEvent::ClosedAndReachedEof,
                            JournalReaderTraceEvent::Completed,
                            JournalWriterTraceEvent::ReinitializeAttempt,
                            JournalWriterTraceEvent::DriverEventAttemptCommit {
                                event: DriverEventKind::Reopened,
                                event_id: repaired_last_event_id,
                                prev_id: 0
                            },
                            JournalWriterTraceEvent::DriverEventCompleted,
                            JournalWriterTraceEvent::ReinitializeComplete,
                        ],
                        "failed at {trim_size} for journal {journal_id}"
                    );
                } else {
                    assert_eq!(
                        &trace[trace.len() - 12..],
                        intovec![
                            JournalReaderTraceEvent::ServerEventAppliedSuccess,
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id - 1), // close event
                            JournalReaderTraceEvent::DriverEventExpectingClose,
                            JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                            JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                            JournalReaderTraceEvent::ClosedAndReachedEof,
                            JournalReaderTraceEvent::Completed,
                            JournalWriterTraceEvent::ReinitializeAttempt,
                            JournalWriterTraceEvent::DriverEventAttemptCommit {
                                event: DriverEventKind::Reopened,
                                event_id: repaired_last_event_id,
                                prev_id: repaired_last_event_id - 1 // close event
                            },
                            JournalWriterTraceEvent::DriverEventCompleted,
                            JournalWriterTraceEvent::ReinitializeComplete
                        ]
                    )
                }
                // now close this so that this works with the post repair handler
                RawJournalWriter::close_driver(&mut jrnl).unwrap();
                let _ = debug_get_offsets();
                let _ = debug_get_trace();
            } else {
                assert_eq!(
                    open_result.unwrap_err().kind(),
                    &ErrorKind::IoError(IoErrorKind::UnexpectedEof.into())
                );
                if repaired_last_event_id == 1 {
                    // empty log, only the reopen
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(0),
                            JournalReaderTraceEvent::DriverEventExpectingClose,
                            JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                            JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                            JournalReaderTraceEvent::DriverEventExpectingReopenBlock,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id)
                        ]
                    );
                } else {
                    assert_eq!(
                        &trace[trace.len() - 5..],
                        intovec![
                            JournalReaderTraceEvent::DriverEventExpectingClose,
                            JournalReaderTraceEvent::DriverEventCompletedBlockRead,
                            JournalReaderTraceEvent::DriverEventExpectedCloseGotClose,
                            JournalReaderTraceEvent::DriverEventExpectingReopenBlock,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id)
                        ]
                    );
                }
            }
        },
        |journal_id, _repaired_last_id, trim_size, repair_result, _db, reopen_result| {
            assert!(reopen_result.is_ok());
            if trim_size == DriverEvent::FULL_EVENT_SIZE {
                // see earlier comment
                assert_eq!(
                    repair_result.unwrap(),
                    RepairResult::NoErrors,
                    "failed at {trim_size} for journal {journal_id}"
                );
            } else {
                assert_eq!(
                    repair_result.unwrap(),
                    RepairResult::UnspecifiedLoss(
                        (DriverEvent::FULL_EVENT_SIZE - trim_size) as u64
                    )
                );
            }
            let _ = debug_get_trace();
            let _ = debug_get_offsets();
        },
    )
}

#[test]
fn corruption_at_runtime() {
    // first get the offsets to compute the size of the event
    const KEY: &str = "hello, universe";
    let offset = {
        debug_set_offset_tracking(true);
        let mut sdb = SimpleDB::new();
        let mut jrnl = create_journal("corruption_at_runtime_test_log.db").unwrap();
        sdb.push(&mut jrnl, KEY).unwrap();
        let (_, offset) = debug_get_offsets().pop_last().unwrap();
        let ret =
            offset as usize - <<SimpleDBJournal as RawJournalAdapter>::Spec as FileSpecV1>::SIZE;
        debug_set_offset_tracking(false);
        let _ = debug_get_trace();
        ret
    };
    let initializers = vec![
        Initializer::new(
            "corruption_at_runtime_open_commit_corrupt",
            |jrnl_id| {
                let mut sdb = SimpleDB::new();
                let mut jrnl = create_journal(jrnl_id)?;
                sdb.push(&mut jrnl, KEY)?;
                // don't close
                Ok(0)
            },
            offset,
        ),
        Initializer::new(
            "corruption_at_runtime_open_multi_commit_then_corrupt",
            |jrnl_id| {
                let mut op_count = 0;
                let mut sdb = SimpleDB::new();
                let mut jrnl = create_journal(jrnl_id)?;
                for _ in 0..TRIALS {
                    sdb.push(&mut jrnl, KEY)?;
                    op_count += 1;
                }
                // don't close
                Ok(op_count)
            },
            offset,
        ),
    ];
    emulate_final_event_corruption(
        initializers,
        |journal_id, repaired_last_event_id, trim_size, _db, open_result, _modstat| {
            let trace = debug_get_trace();
            let err = open_result.unwrap_err();
            assert_eq!(
                err.kind(),
                &ErrorKind::IoError(IoErrorKind::UnexpectedEof.into()),
                "failed for journal {journal_id} with trim_size {trim_size}"
            );
            if trim_size > offset - (sizeof!(u128) + sizeof!(u64)) {
                if repaired_last_event_id == 0 {
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent,
                        ],
                        "failed for journal {journal_id} with trim_size {trim_size}"
                    )
                } else {
                    assert_eq!(
                        &trace[trace.len() - 4..],
                        intovec![
                            JournalReaderTraceEvent::DetectedServerEvent,
                            JournalReaderTraceEvent::ServerEventMetadataParsed,
                            JournalReaderTraceEvent::ServerEventAppliedSuccess,
                            JournalReaderTraceEvent::LookingForEvent,
                        ],
                        "failed for journal {journal_id} with trim_size {trim_size}"
                    )
                }
            } else {
                if repaired_last_event_id == 0 {
                    // empty log
                    assert_eq!(
                        trace,
                        intovec![
                            JournalReaderTraceEvent::Initialized,
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(0),
                            JournalReaderTraceEvent::DetectedServerEvent,
                            JournalReaderTraceEvent::ServerEventMetadataParsed,
                        ],
                        "failed for journal {journal_id} with trim_size {trim_size}"
                    );
                } else {
                    assert_eq!(
                        &trace[trace.len() - 4..],
                        intovec![
                            JournalReaderTraceEvent::LookingForEvent,
                            JournalReaderTraceEvent::AttemptingEvent(repaired_last_event_id - 1),
                            JournalReaderTraceEvent::DetectedServerEvent,
                            JournalReaderTraceEvent::ServerEventMetadataParsed,
                        ],
                        "failed for journal {journal_id} with trim_size {trim_size}"
                    );
                }
            }
        },
        |journal_id, _repaired_last_id, trim_size, repair_result, _db, reopen_result| {
            assert!(reopen_result.is_ok());
            assert_eq!(
                repair_result.unwrap(),
                RepairResult::UnspecifiedLoss((offset - trim_size) as u64),
                "failed for journal {journal_id} with trim_size {trim_size}"
            );
            let _ = debug_get_trace();
        },
    )
}

#[test]
fn midway_corruption_close() {
    let initializers = vec![
        Initializer::new_driver_type("midway_corruption_close_direct", |jrnl_id| {
            // create and close
            let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
            RawJournalWriter::close_driver(&mut jrnl)?;
            drop(jrnl);
            // reopen and close
            let mut jrnl = open_journal::<SimpleDBJournal>(
                jrnl_id,
                &SimpleDB::new(),
                JournalSettings::default(),
            )?;
            RawJournalWriter::close_driver(&mut jrnl)?;
            drop(jrnl);
            Ok(0) // close (to corrupt), reopen, close
        }),
        Initializer::new_driver_type(
            "midway_corruption_close_events_before_second_close",
            |jrnl_id| {
                {
                    // create and close
                    let mut jrnl = create_journal::<SimpleDBJournal>(jrnl_id)?;
                    RawJournalWriter::close_driver(&mut jrnl)?;
                }
                {
                    // reopen and close
                    let mut jrnl = open_journal::<SimpleDBJournal>(
                        jrnl_id,
                        &SimpleDB::new(),
                        JournalSettings::default(),
                    )?;
                    RawJournalWriter::close_driver(&mut jrnl)?; // <-- corrupt this one
                }
                {
                    let mut jrnl = open_journal::<SimpleDBJournal>(
                        jrnl_id,
                        &SimpleDB::new(),
                        JournalSettings::default(),
                    )?;
                    let _ = apply_event_mix(&mut jrnl)?;
                    RawJournalWriter::close_driver(&mut jrnl)?;
                }
                Ok(2) // corrupt the second close event
            },
        ),
    ];
    debug_set_offset_tracking(true);
    emulate_midway_corruption(
        initializers,
        |journal_id, _last_id, trim_size, db, open_result, _modstat| {
            assert!(
                open_result.is_err(),
                "failed for journal {journal_id} with trim_size {trim_size}"
            );
            // all data will be lost, so the DB will be empty
            assert_eq!(
                db.data().len(),
                0,
                "failed for journal {journal_id} with trim_size {trim_size}"
            );
            let _ = debug_get_offsets();
            let _ = debug_get_trace();
        },
        |journal_id, last_id, trim_size, repair_result, db, reopen_result| {
            let _ = reopen_result.unwrap();
            // all data will be lost, so the DB will be empty
            assert_eq!(
                db.data().len(),
                0,
                "failed for journal {journal_id} with trim_size {trim_size}"
            );
            if last_id == 0 {
                assert_eq!(
                    repair_result.unwrap(),
                    RepairResult::UnspecifiedLoss(
                        ((DriverEvent::FULL_EVENT_SIZE * 3) - trim_size) as u64
                    ),
                    "failed for journal {journal_id} with trim_size {trim_size}"
                );
            } else {
                // this is a serious midway corruption with major data loss
                let full_log_size = File::open(journal_id).unwrap().f_len().unwrap();
                assert_eq!(
                    repair_result.unwrap(),
                    RepairResult::UnspecifiedLoss(
                        full_log_size
                            - <<SimpleDBJournal as RawJournalAdapter>::Spec as FileSpecV1>::SIZE // account for header
                                as u64
                            - (DriverEvent::FULL_EVENT_SIZE * 2) as u64 // account for close (0), reopen(1)
                            - trim_size as u64 // account for trim
                    ),
                    "failed for journal {journal_id} with trim_size {trim_size}"
                );
            }
            let _ = debug_get_trace();
            let _ = debug_get_offsets();
        },
    );
    debug_set_offset_tracking(false);
}
