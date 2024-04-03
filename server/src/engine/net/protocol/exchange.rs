/*
 * Created on Tue Apr 02 2024
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
    crate::{engine::mem::BufferedScanner, util::compiler},
    core::fmt,
};

/*
    Skyhash/2.1 Implementation
    ---
    This is an implementation of Skyhash/2.1, Skytable's data exchange protocol.

    0. Notes
    ++++++++++++++++++
    - 2.1 is fully backwards compatible with 2.0 clients. As such we don't even designate it as a separate version.
    - The "LF exception" essentially allows `0\n` to be equal to `\n`. It's unimportant to enforce this

    1.1 Query Types
    ++++++++++++++++++
    The protocol makes two distinctions, at the protocol-level about the type of queries:
    a. Simple query
    b. Pipeline

    1.1.1 Simple Query
    ++++++++++++++++++
    A simple query
*/

/*
    sq definition
*/

#[derive(Debug, PartialEq)]
pub struct SQuery<'a> {
    buf: &'a [u8],
    q_window: usize,
}

impl<'a> SQuery<'a> {
    fn new(buf: &'a [u8], q_window: usize) -> Self {
        Self { buf, q_window }
    }
    pub fn query(&self) -> &[u8] {
        &self.buf[..self.q_window]
    }
    pub fn params(&self) -> &[u8] {
        &self.buf[self.q_window..]
    }
}

/*
    scanint
*/

fn scan_usize_guaranteed_termination(
    scanner: &mut BufferedScanner,
) -> Result<usize, ExchangeError> {
    let mut ret = 0usize;
    let mut stop = scanner.rounded_eq(b'\n');
    while !scanner.eof() & !stop {
        let next_byte = unsafe {
            // UNSAFE(@ohsayan): loop invariant
            scanner.next_byte()
        };
        match ret
            .checked_mul(10)
            .map(|int| int.checked_add((next_byte & 0x0f) as usize))
        {
            Some(Some(int)) if next_byte.is_ascii_digit() => ret = int,
            _ => return Err(ExchangeError::NotAsciiByteOrOverflow),
        }
        stop = scanner.rounded_eq(b'\n');
    }
    unsafe {
        // UNSAFE(@ohsayan): scanned stop, but not accounted for yet
        scanner.incr_cursor_if(stop)
    }
    if stop {
        Ok(ret)
    } else {
        Err(ExchangeError::UnterminatedInteger)
    }
}

#[derive(Clone, Copy, PartialEq)]
struct Usize {
    v: isize,
}

impl Usize {
    const SHIFT: u32 = isize::BITS - 1;
    const MASK: isize = 1 << Self::SHIFT;
    #[inline(always)]
    const fn new(v: isize) -> Self {
        Self { v }
    }
    #[inline(always)]
    const fn new_unflagged(int: usize) -> Self {
        Self::new(int as isize)
    }
    #[inline(always)]
    fn int(&self) -> usize {
        (self.v & !Self::MASK) as usize
    }
    #[inline(always)]
    fn update(&mut self, new: usize) {
        self.v = (new as isize) | (self.v & Self::MASK);
    }
    #[inline(always)]
    fn flag(&self) -> bool {
        (self.v & Self::MASK) != 0
    }
    #[inline(always)]
    fn set_flag_if(&mut self, iff: bool) {
        self.v |= (iff as isize) << Self::SHIFT;
    }
}

impl fmt::Debug for Usize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Usize")
            .field("int", &self.int())
            .field("flag", &self.flag())
            .finish()
    }
}

impl Usize {
    /// Attempt to "complete" a scan of the integer. Idempotency guarantee: it is guaranteed that calls would not change the state
    /// of the [`Usize`] or the buffer if the final state has been reached
    fn update_scanned(&mut self, scanner: &mut BufferedScanner) -> Result<(), ()> {
        let mut stop = scanner.rounded_eq(b'\n');
        while !stop & !scanner.eof() & !self.flag() {
            let byte = unsafe {
                // UNSAFE(@ohsayan): verified by loop invariant
                scanner.next_byte()
            };
            match (self.int() as isize) // this cast allows us to guarantee that we don't trip the flag
                .checked_mul(10)
                .map(|int| int.checked_add((byte & 0x0f) as isize))
            {
                Some(Some(int)) if byte.is_ascii_digit() => self.update(int as usize),
                _ => return Err(()),
            }
            stop = scanner.rounded_eq(b'\n');
        }
        unsafe {
            // UNSAFE(@ohsayan): scanned stop byte but did not account for it; the flag check is for cases where the input buffer
            // has something like [LF][LF] in which case we stopped at the first LF but we would accidentally read the second one
            // on the second derogatory call
            scanner.incr_cursor_if(stop & !self.flag())
        }
        self.set_flag_if(stop | self.flag()); // if second call we must check the flag
        Ok(())
    }
}

/*
    states
*/

#[derive(Debug, PartialEq)]
pub enum ExchangeState {
    Initial,
    Simple(SQState),
    Pipeline(PipeState),
}

#[derive(Debug, PartialEq)]
pub struct SQState {
    packet_s: Usize,
}

impl SQState {
    const fn new(packet_s: Usize) -> Self {
        Self { packet_s }
    }
}

#[derive(Debug, PartialEq)]
pub struct PipeState {
    packet_s: Usize,
}

impl PipeState {
    const fn new(packet_s: Usize) -> Self {
        Self { packet_s }
    }
}

impl Default for ExchangeState {
    fn default() -> Self {
        Self::Initial
    }
}

#[derive(Debug, PartialEq)]
pub enum ExchangeResult<'a> {
    NewState(ExchangeState),
    Simple(SQuery<'a>),
    Pipeline(Pipeline<'a>),
}

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum ExchangeError {
    UnknownFirstByte,
    NotAsciiByteOrOverflow,
    UnterminatedInteger,
    IncorrectQuerySizeOrMoreBytes,
}

pub struct Exchange<'a> {
    scanner: BufferedScanner<'a>,
}

impl<'a> Exchange<'a> {
    const MIN_Q_SIZE: usize = "P0\n".len();
    fn new(scanner: BufferedScanner<'a>) -> Self {
        Self { scanner }
    }
    pub fn try_complete(
        scanner: BufferedScanner<'a>,
        state: ExchangeState,
    ) -> Result<(ExchangeResult, usize), ExchangeError> {
        Self::new(scanner).complete(state)
    }
}

impl<'a> Exchange<'a> {
    fn complete(
        mut self,
        state: ExchangeState,
    ) -> Result<(ExchangeResult<'a>, usize), ExchangeError> {
        match state {
            ExchangeState::Initial => {
                if compiler::likely(self.scanner.has_left(Self::MIN_Q_SIZE)) {
                    let first_byte = unsafe {
                        // UNSAFE(@ohsayan): already verified in above branch
                        self.scanner.next_byte()
                    };
                    match first_byte {
                        b'S' => self.process_simple(SQState::new(Usize::new_unflagged(0))),
                        b'P' => self.process_pipe(PipeState::new(Usize::new_unflagged(0))),
                        _ => return Err(ExchangeError::UnknownFirstByte),
                    }
                } else {
                    Ok(ExchangeResult::NewState(state))
                }
            }
            ExchangeState::Simple(sq_s) => self.process_simple(sq_s),
            ExchangeState::Pipeline(pipe_s) => self.process_pipe(pipe_s),
        }
        .map(|ret| (ret, self.scanner.cursor()))
    }
    fn process_simple(
        &mut self,
        mut sq_state: SQState,
    ) -> Result<ExchangeResult<'a>, ExchangeError> {
        // try to complete the packet size if needed
        sq_state
            .packet_s
            .update_scanned(&mut self.scanner)
            .map_err(|_| ExchangeError::NotAsciiByteOrOverflow)?;
        if sq_state.packet_s.flag() & self.scanner.has_left(sq_state.packet_s.int()) {
            // we have the full packet size and the required data
            let q_window = scan_usize_guaranteed_termination(&mut self.scanner)?;
            let nonzero = (q_window != 0) & (sq_state.packet_s.int() != 0);
            if compiler::likely(self.scanner.remaining_size_is(sq_state.packet_s.int()) & nonzero) {
                // this check is important because the client might have given us an incorrect q size
                Ok(ExchangeResult::Simple(SQuery::new(
                    self.scanner.current_buffer(),
                    q_window,
                )))
            } else {
                Err(ExchangeError::IncorrectQuerySizeOrMoreBytes)
            }
        } else {
            Ok(ExchangeResult::NewState(ExchangeState::Simple(sq_state)))
        }
    }
    fn process_pipe(&mut self, mut pipe_s: PipeState) -> Result<ExchangeResult<'a>, ExchangeError> {
        // try to complete the packet size if needed
        pipe_s
            .packet_s
            .update_scanned(&mut self.scanner)
            .map_err(|_| ExchangeError::NotAsciiByteOrOverflow)?;
        if pipe_s.packet_s.flag() & self.scanner.remaining_size_is(pipe_s.packet_s.int()) {
            // great, we have the entire packet
            Ok(ExchangeResult::Pipeline(Pipeline::new(
                self.scanner.current_buffer(),
            )))
        } else {
            Ok(ExchangeResult::NewState(ExchangeState::Pipeline(pipe_s)))
        }
    }
}

/*
    pipeline
*/

#[derive(Debug, PartialEq)]
pub struct Pipeline<'a> {
    scanner: BufferedScanner<'a>,
}

impl<'a> Pipeline<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            scanner: BufferedScanner::new(buf),
        }
    }
    pub fn next_query(&mut self) -> Result<Option<SQuery<'a>>, ExchangeError> {
        let nonzero = self.scanner.buffer_len() != 0;
        if self.scanner.eof() & nonzero {
            Ok(None)
        } else {
            let query_size = scan_usize_guaranteed_termination(&mut self.scanner)?;
            let param_size = scan_usize_guaranteed_termination(&mut self.scanner)?;
            let (full_size, overflow) = param_size.overflowing_add(query_size);
            if compiler::likely(self.scanner.has_left(full_size) & !overflow) {
                Ok(Some(SQuery {
                    buf: self.scanner.current_buffer(),
                    q_window: query_size,
                }))
            } else {
                Err(ExchangeError::IncorrectQuerySizeOrMoreBytes)
            }
        }
    }
}
