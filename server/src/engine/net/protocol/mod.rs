/*
 * Created on Fri Sep 15 2023
 *
 * This file is a part of Skytable
 * Skytable (formerly known as TerrabaseDB or Skybase) is a free and open-source
 * NoSQL database written by Sayan Nandan ("the Author") with the
 * vision to provide flexibility in data modelling without compromising
 * on performance, queryability or scalability.
 *
 * Copyright (c) 2023, Sayan Nandan <ohsayan@outlook.com>
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

/*
 * Implementation of the Skyhash/2.0 Protocol
 * ---
 * This module implements handshake and exchange mode extensions for the Skyhash protocol.
 *
 * Notable points:
 * - [Deprecated] Newline exception: while all integers are to be encoded and postfixed with an LF, a single LF
 * without any integer payload is equivalent to a zero value. we allow this because it's easier to specify formally
 * as states
 * - Handshake parameter versions: We currently only evaluate values for the version "original" (shipped with
 * Skytable 0.8.0)
 * - FIXME(@ohsayan) Optimistic retry without timeout: Our current algorithm does not apply a timeout to receive data
 * and optimistically retries infinitely until the target block size is received
*/

mod exchange;
mod handshake;
#[cfg(test)]
mod tests;

use {
    self::{
        exchange::{Exchange, ExchangeResult, ExchangeState, Pipeline},
        handshake::{
            AuthMode, CHandshake, DataExchangeMode, HandshakeResult, HandshakeState,
            HandshakeVersion, ProtocolError, ProtocolVersion, QueryMode,
        },
    },
    super::{IoResult, QueryLoopResult, Socket},
    crate::engine::{
        self,
        core::system_db::VerifyUser,
        error::{QueryError, QueryResult},
        fractal::{Global, GlobalInstanceLike},
        mem::{BufferedScanner, IntegerRepr},
    },
    bytes::{Buf, BytesMut},
    tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter},
};

// re-export
pub use self::exchange::SQuery;

/*
    connection state
*/

#[derive(Debug, PartialEq)]
pub struct ClientLocalState {
    username: Box<str>,
    root: bool,
    hs: handshake::CHandshakeStatic,
    cs: Option<Box<str>>,
}

impl ClientLocalState {
    pub fn new(username: Box<str>, root: bool, hs: handshake::CHandshakeStatic) -> Self {
        Self {
            username,
            root,
            hs,
            cs: None,
        }
    }
    pub fn is_root(&self) -> bool {
        self.root
    }
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn set_cs(&mut self, new: Box<str>) {
        self.cs = Some(new);
    }
    pub fn unset_cs(&mut self) {
        self.cs = None;
    }
    pub fn get_cs(&self) -> Option<&str> {
        self.cs.as_deref()
    }
}

/*
    handshake
*/

#[derive(Debug, PartialEq)]
enum PostHandshake {
    Okay(ClientLocalState),
    Error(ProtocolError),
    ConnectionClosedFin,
    ConnectionClosedRst,
}

async fn do_handshake<S: Socket>(
    con: &mut BufWriter<S>,
    buf: &mut BytesMut,
    global: &Global,
) -> IoResult<PostHandshake> {
    let mut expected = CHandshake::INITIAL_READ;
    let mut state = HandshakeState::default();
    let mut cursor = 0;
    let handshake;
    loop {
        let read_many = con.read_buf(buf).await?;
        if read_many == 0 {
            if buf.is_empty() {
                return Ok(PostHandshake::ConnectionClosedFin);
            } else {
                return Ok(PostHandshake::ConnectionClosedRst);
            }
        }
        if buf.len() < expected {
            continue;
        }
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match handshake::CHandshake::resume_with(&mut scanner, state) {
            HandshakeResult::Completed(hs) => {
                handshake = hs;
                cursor = scanner.cursor();
                break;
            }
            HandshakeResult::ChangeState { new_state, expect } => {
                expected = expect;
                state = new_state;
                cursor = scanner.cursor();
            }
            HandshakeResult::Error(e) => {
                return Ok(PostHandshake::Error(e));
            }
        }
    }
    // check handshake
    if cfg!(debug_assertions) {
        assert_eq!(
            handshake.hs_static().hs_version(),
            HandshakeVersion::Original
        );
        assert_eq!(handshake.hs_static().protocol(), ProtocolVersion::Original);
        assert_eq!(
            handshake.hs_static().exchange_mode(),
            DataExchangeMode::QueryTime
        );
        assert_eq!(handshake.hs_static().query_mode(), QueryMode::Bql1);
        assert_eq!(handshake.hs_static().auth_mode(), AuthMode::Password);
    }
    match core::str::from_utf8(handshake.hs_auth().username()) {
        Ok(uname) => {
            match global
                .state()
                .namespace()
                .sys_db()
                .verify_user(uname, handshake.hs_auth().password())
            {
                okay @ (VerifyUser::Okay | VerifyUser::OkayRoot) => {
                    let hs = handshake.hs_static();
                    let ret = Ok(PostHandshake::Okay(ClientLocalState::new(
                        uname.into(),
                        okay.is_root(),
                        hs,
                    )));
                    buf.advance(cursor);
                    return ret;
                }
                VerifyUser::IncorrectPassword | VerifyUser::NotFound => {}
            }
        }
        Err(_) => {}
    };
    Ok(PostHandshake::Error(ProtocolError::RejectAuth))
}

/*
    exec event loop
*/

async fn cleanup_for_next_query<S: Socket>(
    con: &mut BufWriter<S>,
    buf: &mut BytesMut,
) -> IoResult<(ExchangeState, usize)> {
    con.flush().await?; // flush write buffer
    buf.clear(); // clear read buffer
    Ok((ExchangeState::default(), 0))
}

pub(super) async fn query_loop<S: Socket>(
    con: &mut BufWriter<S>,
    buf: &mut BytesMut,
    global: &Global,
) -> IoResult<QueryLoopResult> {
    // handshake
    let mut client_state = match do_handshake(con, buf, global).await? {
        PostHandshake::Okay(hs) => hs,
        PostHandshake::ConnectionClosedFin => return Ok(QueryLoopResult::Fin),
        PostHandshake::ConnectionClosedRst => return Ok(QueryLoopResult::Rst),
        PostHandshake::Error(e) => {
            // failed to handshake; we'll close the connection
            let hs_err_packet = [b'H', 0, 1, e.value_u8()];
            con.write_all(&hs_err_packet).await?;
            return Ok(QueryLoopResult::HSFailed);
        }
    };
    // done handshaking
    con.write_all(b"H\x00\x00\x00").await?;
    con.flush().await?;
    let mut state = ExchangeState::default();
    let mut cursor = 0;
    loop {
        if con.read_buf(buf).await? == 0 {
            if buf.is_empty() {
                return Ok(QueryLoopResult::Fin);
            } else {
                return Ok(QueryLoopResult::Rst);
            }
        }
        match Exchange::try_complete(
            unsafe {
                // UNSAFE(@ohsayan): the cursor is either 0 or returned by the exchange impl
                BufferedScanner::new_with_cursor(&buf, cursor)
            },
            state,
        ) {
            Ok((result, new_cursor)) => match result {
                ExchangeResult::NewState(new_state) => {
                    state = new_state;
                    cursor = new_cursor;
                }
                ExchangeResult::Simple(query) => {
                    exec_simple(con, &mut client_state, global, query).await?;
                    (state, cursor) = cleanup_for_next_query(con, buf).await?;
                }
                ExchangeResult::Pipeline(pipe) => {
                    exec_pipe(con, &mut client_state, global, pipe).await?;
                    (state, cursor) = cleanup_for_next_query(con, buf).await?;
                }
            },
            Err(()) => {
                // respond with error
                let [a, b] = (QueryError::SysNetworkSystemIllegalClientPacket.value_u8() as u16)
                    .to_le_bytes();
                con.write_all(&[ResponseType::Error.value_u8(), a, b])
                    .await?;
                (state, cursor) = cleanup_for_next_query(con, buf).await?;
            }
        }
    }
}

/*
    responses
*/

#[repr(u8)]
#[derive(sky_macros::EnumMethods, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[allow(unused)]
pub enum ResponseType {
    Null = 0x00,
    Bool = 0x01,
    UInt8 = 0x02,
    UInt16 = 0x03,
    UInt32 = 0x04,
    UInt64 = 0x05,
    SInt8 = 0x06,
    SInt16 = 0x07,
    SInt32 = 0x08,
    SInt64 = 0x09,
    Float32 = 0x0A,
    Float64 = 0x0B,
    Binary = 0x0C,
    String = 0x0D,
    List = 0x0E,
    Dict = 0x0F,
    Error = 0x10,
    Row = 0x11,
    Empty = 0x12,
    MultiRow = 0x13,
}

#[derive(Debug, PartialEq)]
pub enum Response {
    Empty,
    Null,
    Serialized {
        ty: ResponseType,
        size: usize,
        data: Vec<u8>,
    },
    Bool(bool),
}

async fn write_response<S: Socket>(
    resp: QueryResult<Response>,
    con: &mut BufWriter<S>,
) -> IoResult<()> {
    match resp {
        Ok(Response::Empty) => con.write_all(&[ResponseType::Empty.value_u8()]).await,
        Ok(Response::Serialized { ty, size, data }) => {
            con.write_u8(ty.value_u8()).await?;
            let mut irep = IntegerRepr::new();
            con.write_all(irep.as_bytes(size as u64)).await?;
            con.write_u8(b'\n').await?;
            con.write_all(&data).await
        }
        Ok(Response::Bool(b)) => {
            con.write_all(&[ResponseType::Bool.value_u8(), b as u8])
                .await
        }
        Ok(Response::Null) => con.write_u8(ResponseType::Null.value_u8()).await,
        Err(e) => {
            let [a, b] = (e.value_u8() as u16).to_le_bytes();
            con.write_all(&[ResponseType::Error.value_u8(), a, b]).await
        }
    }
}

/*
    simple query
*/

async fn exec_simple<S: Socket>(
    con: &mut BufWriter<S>,
    cs: &mut ClientLocalState,
    global: &Global,
    query: SQuery<'_>,
) -> IoResult<()> {
    write_response(
        engine::core::exec::dispatch_to_executor(global, cs, query).await,
        con,
    )
    .await
}

/*
    pipeline
*/

async fn exec_pipe<'a, S: Socket>(
    con: &mut BufWriter<S>,
    cs: &mut ClientLocalState,
    global: &Global,
    mut pipe: Pipeline<'a>,
) -> IoResult<()> {
    loop {
        match pipe.next_query() {
            Ok(None) => break Ok(()),
            Ok(Some(q)) => {
                write_response(
                    engine::core::exec::dispatch_to_executor(global, cs, q).await,
                    con,
                )
                .await?
            }
            Err(()) => {
                // respond with error
                let [a, b] = (QueryError::SysNetworkSystemIllegalClientPacket.value_u8() as u16)
                    .to_le_bytes();
                con.write_all(&[ResponseType::Error.value_u8(), a, b])
                    .await?;
                break Ok(());
            }
        }
    }
}
