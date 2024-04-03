/*
 * Created on Mon Sep 18 2023
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

use {
    super::handshake::ProtocolError,
    crate::engine::{
        mem::BufferedScanner,
        net::protocol::handshake::{
            AuthMode, CHandshake, CHandshakeAuth, CHandshakeStatic, DataExchangeMode,
            HandshakeResult, HandshakeState, HandshakeVersion, ProtocolVersion, QueryMode,
        },
    },
};

pub(super) fn create_simple_query<const N: usize>(query: &str, params: [&str; N]) -> Vec<u8> {
    let mut buf = vec![];
    let query_size_as_string = query.len().to_string();
    let total_packet_size = query.len()
        + params.iter().map(|l| l.len()).sum::<usize>()
        + query_size_as_string.len()
        + 1;
    // segment 1
    buf.push(b'S');
    buf.extend(total_packet_size.to_string().as_bytes());
    buf.push(b'\n');
    // segment
    buf.extend(query_size_as_string.as_bytes());
    buf.push(b'\n');
    // dataframe
    buf.extend(query.as_bytes());
    params
        .into_iter()
        .for_each(|param| buf.extend(param.as_bytes()));
    buf
}

/*
    client handshake
*/

const FULL_HANDSHAKE_WITH_AUTH: [u8; 23] = *b"H\0\0\0\0\05\n8\nsayanpass1234";

const STATIC_HANDSHAKE_WITH_AUTH: CHandshakeStatic = CHandshakeStatic::new(
    HandshakeVersion::Original,
    ProtocolVersion::Original,
    DataExchangeMode::QueryTime,
    QueryMode::Bql1,
    AuthMode::Password,
);

/*
    handshake with no state changes
*/

#[test]
fn parse_staged_with_auth() {
    for i in 0..FULL_HANDSHAKE_WITH_AUTH.len() {
        let buf = &FULL_HANDSHAKE_WITH_AUTH[..i + 1];
        let mut s = BufferedScanner::new(buf);
        let ref mut scanner = s;
        let result = CHandshake::resume_with(scanner, HandshakeState::Initial);
        match buf.len() {
            1..=5 => {
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::Initial,
                        expect: CHandshake::INITIAL_READ
                    }
                );
            }
            6..=9 => {
                // might seem funny that we don't parse the second integer at all, but it's because
                // of the relatively small size of the integers
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::StaticBlock(STATIC_HANDSHAKE_WITH_AUTH),
                        expect: 4
                    }
                );
            }
            10..=22 => {
                assert_eq!(
                    result,
                    HandshakeResult::ChangeState {
                        new_state: HandshakeState::ExpectingVariableBlock {
                            static_hs: STATIC_HANDSHAKE_WITH_AUTH,
                            uname_l: 5,
                            pwd_l: 8
                        },
                        expect: 13,
                    }
                );
            }
            23 => {
                assert_eq!(
                    result,
                    HandshakeResult::Completed(CHandshake::new(
                        STATIC_HANDSHAKE_WITH_AUTH,
                        CHandshakeAuth::new(b"sayan", b"pass1234")
                    ))
                );
            }
            _ => unreachable!(),
        }
    }
}

/*
    handshake with state changes
*/

fn run_state_changes_return_rounds(src: &[u8], expected_final_handshake: CHandshake) -> usize {
    let mut rounds = 0;
    let mut state = HandshakeState::default();
    let mut cursor = 0;
    let mut expect_many = CHandshake::INITIAL_READ;
    loop {
        rounds += 1;
        let buf = &src[..cursor + expect_many];
        let mut scanner = unsafe { BufferedScanner::new_with_cursor(buf, cursor) };
        match CHandshake::resume_with(&mut scanner, state) {
            HandshakeResult::ChangeState { new_state, expect } => {
                state = new_state;
                expect_many = expect;
                cursor = scanner.cursor();
            }
            HandshakeResult::Completed(hs) => {
                assert_eq!(hs, expected_final_handshake);
                break;
            }
            HandshakeResult::Error(e) => panic!("unexpected handshake error: {:?}", e),
        }
    }
    rounds
}

#[test]
fn parse_auth_with_state_updates() {
    let rounds = run_state_changes_return_rounds(
        &FULL_HANDSHAKE_WITH_AUTH,
        CHandshake::new(
            STATIC_HANDSHAKE_WITH_AUTH,
            CHandshakeAuth::new(b"sayan", b"pass1234"),
        ),
    );
    assert_eq!(rounds, 3); // r1 = initial read, r2 = lengths, r3 = items
}

const HS_BAD_PACKET: [u8; 6] = *b"I\x00\0\0\0\0";
const HS_BAD_VERSION_HS: [u8; 6] = *b"H\x01\0\0\0\0";
const HS_BAD_VERSION_PROTO: [u8; 6] = *b"H\0\x01\0\0\0";
const HS_BAD_MODE_XCHG: [u8; 6] = *b"H\0\0\x01\0\0";
const HS_BAD_MODE_QUERY: [u8; 6] = *b"H\0\0\0\x01\0";
const HS_BAD_MODE_AUTH: [u8; 6] = *b"H\0\0\0\0\x01";

fn scan_hs(hs: impl AsRef<[u8]>, f: impl Fn(HandshakeResult)) {
    let mut scanner = BufferedScanner::new(hs.as_ref());
    let hs = CHandshake::resume_with(&mut scanner, Default::default());
    f(hs)
}

#[test]
fn hs_bad_packet_illegal_username_length() {
    scan_hs(b"H\0\0\0\0\0A\n8\nsayanpass1234", |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_packet_illegal_password_length() {
    scan_hs(b"H\0\0\0\0\05\nA\nsayanpass1234", |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_packet_illegal_pwd_uname_length() {
    scan_hs(b"H\0\0\0\0\0A\nA\nsayanpass1234", |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_packet_first_byte() {
    scan_hs(HS_BAD_PACKET, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::CorruptedHSPacket)
        )
    })
}

#[test]
fn hs_bad_version_hs() {
    scan_hs(HS_BAD_VERSION_HS, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectHSVersion)
        )
    })
}

#[test]
fn hs_bad_version_proto() {
    scan_hs(HS_BAD_VERSION_PROTO, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectProtocol)
        )
    })
}

#[test]
fn hs_bad_exchange_mode() {
    scan_hs(HS_BAD_MODE_XCHG, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectExchangeMode)
        )
    })
}

#[test]
fn hs_bad_query_mode() {
    scan_hs(HS_BAD_MODE_QUERY, |hs_result| {
        assert_eq!(
            hs_result,
            HandshakeResult::Error(ProtocolError::RejectQueryMode)
        )
    })
}

#[test]
fn hs_bad_auth_mode() {
    scan_hs(HS_BAD_MODE_AUTH, |hs_result| {
        assert_eq!(hs_result, HandshakeResult::Error(ProtocolError::RejectAuth))
    })
}
