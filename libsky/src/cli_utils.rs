/*
 * This file is a part of Skytable
 *
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
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    str::FromStr,
};

/*
    cli args traits & types
*/

pub type CliResult<T> = Result<T, CliArgsError>;

#[derive(Debug)]
pub enum CliArgsError {
    ArgFmtError(String),
    DuplicateFlag(String),
    DuplicateOption(String),
    SubcommandDisallowed,
    ArgParseError(String),
}

pub trait CliArgsDecode: Sized {
    type Data;
    fn initialize<const SWITCH: bool>(iter: &mut impl Iterator<Item = impl ArgItem>) -> Self::Data;
    fn push_flag(data: &mut Self::Data, flag: Box<str>) -> CliResult<()>;
    fn push_option(
        data: &mut Self::Data,
        option_name: Box<str>,
        option_value: Box<str>,
    ) -> CliResult<()>;
    fn yield_subcommand(
        data: Self::Data,
        subcommand: Box<str>,
        args: impl IntoIterator<Item = impl ArgItem>,
    ) -> CliResult<Self>;
    fn yield_command(data: Self::Data) -> CliResult<Self>;
    fn yield_help(data: Self::Data) -> CliResult<Self>;
}

pub trait CommandLineArgs: Sized + CliArgsDecode {
    fn parse(src: impl IntoIterator<Item = impl ArgItem>) -> CliResult<Self> {
        decode_args::<Self, true>(src)
    }
    fn from_cli() -> CliResult<Self> {
        Self::parse(std::env::args())
    }
}

impl<T: Sized + CliArgsDecode> CommandLineArgs for T {}

/*
    helper traits
*/

pub trait ArgItem {
    fn as_str(&self) -> &str;
    fn boxed_str(self) -> Box<str>;
}

impl<'a> ArgItem for &'a str {
    fn as_str(&self) -> &str {
        self
    }
    fn boxed_str(self) -> Box<str> {
        self.to_owned().into_boxed_str()
    }
}

impl ArgItem for String {
    fn as_str(&self) -> &str {
        self
    }
    fn boxed_str(self) -> Box<str> {
        self.into_boxed_str()
    }
}

/*
    args decoder
*/

fn decode_args<C: CliArgsDecode, const HAS_BINARY_NAME: bool>(
    src: impl IntoIterator<Item = impl ArgItem>,
) -> CliResult<C> {
    let mut args = src.into_iter().peekable();
    if HAS_BINARY_NAME {
        // must not be empty
        if args.peek().is_none() {
            return Err(CliArgsError::ArgFmtError(
                "expected arguments but found none".to_owned(),
            ));
        }
    }
    let mut cli_data = C::initialize::<HAS_BINARY_NAME>(&mut args);
    while let Some(arg) = args.next() {
        let arg = arg.as_str();
        if arg == "-h" || arg == "--help" {
            return C::yield_help(cli_data);
        }
        if arg.starts_with("--") {
            // option or flag
            let arg = &arg[2..];
            if arg.is_empty() {
                return Err(CliArgsError::ArgFmtError(format!("invalid argument")));
            }
            // is this arg in the --x=y format?
            let mut arg_split = arg.split("=");
            let (arg_split_name_, arg_split_value_) = (arg_split.next(), arg_split.next());
            match (arg_split_name_, arg_split_value_) {
                (Some(name_), Some(value_)) => {
                    if name_.is_empty() || value_.is_empty() {
                        return Err(CliArgsError::ArgFmtError(format!(
                            "the argument `{arg}` was formatted incorrectly"
                        )));
                    }
                    // yes, it was formatted this way
                    C::push_option(&mut cli_data, name_.boxed_str(), value_.boxed_str())?;
                    continue;
                }
                (Some(_), None) => {}
                _ => unreachable!(),
            }
            // no, probably in the --x y format
            match args.peek() {
                Some(arg_) => {
                    if arg_.as_str().starts_with("--") || arg_.as_str().starts_with("-") {
                        // flag
                        C::push_flag(&mut cli_data, arg.boxed_str())?;
                    } else {
                        // option
                        C::push_option(
                            &mut cli_data,
                            arg.boxed_str(),
                            args.next().unwrap().boxed_str(),
                        )?;
                    }
                }
                None => {
                    // flag
                    C::push_flag(&mut cli_data, arg.boxed_str())?;
                }
            }
        } else {
            // this is subcommand
            return C::yield_subcommand(cli_data, arg.boxed_str(), args);
        }
    }
    C::yield_command(cli_data)
}

/*
    cli arg impl: CliCommand (simple, subcommand-less)
*/

#[derive(Debug, PartialEq)]
pub enum CliCommand {
    Help(CliCommandData),
    Run(CliCommandData),
}

#[derive(Debug, PartialEq, Clone)]
pub struct CliCommandData {
    options: HashMap<Box<str>, Box<str>>,
    flags: HashSet<Box<str>>,
}

impl CliCommandData {
    pub fn is_empty(&self) -> bool {
        self.options.is_empty() && self.flags.is_empty()
    }
    pub fn take_flag(&mut self, name: &str) -> bool {
        self.flags.remove(name)
    }
    pub fn take_option(&mut self, option_name: &str) -> Option<Box<str>> {
        self.options.remove(option_name)
    }
    pub fn take_option_into<T: FromStr>(&mut self, option_name: &str) -> CliResult<Option<T>> {
        match self.take_option(option_name).map(|s| s.parse()) {
            None => Ok(None),
            Some(Ok(v)) => Ok(Some(v)),
            Some(Err(_)) => Err(CliArgsError::ArgParseError(format!(
                "failed to parse option `{option_name}`"
            ))),
        }
    }
}

impl CliArgsDecode for CliCommand {
    type Data = CliCommandData;
    fn initialize<const SWITCH: bool>(
        iter: &mut impl Iterator<Item = impl ArgItem>,
    ) -> CliCommandData {
        if SWITCH {
            let _binary_name = iter.next();
        }
        CliCommandData {
            options: Default::default(),
            flags: Default::default(),
        }
    }
    fn push_flag(data: &mut Self::Data, flag: Box<str>) -> CliResult<()> {
        if !data.flags.insert(flag.to_owned()) {
            return Err(CliArgsError::DuplicateFlag(format!(
                "found duplicate flag --{flag}"
            )));
        }
        Ok(())
    }
    fn push_option(
        data: &mut Self::Data,
        option_name: Box<str>,
        option_value: Box<str>,
    ) -> CliResult<()> {
        match data.options.entry(option_name) {
            Entry::Vacant(ve) => {
                ve.insert(option_value.to_owned());
                Ok(())
            }
            Entry::Occupied(oe) => Err(CliArgsError::DuplicateOption(format!(
                "found duplicate option --{}",
                oe.key()
            ))),
        }
    }
    fn yield_subcommand(
        _: Self::Data,
        _: Box<str>,
        _: impl IntoIterator<Item = impl ArgItem>,
    ) -> CliResult<Self> {
        return Err(CliArgsError::SubcommandDisallowed);
    }
    fn yield_command(data: Self::Data) -> CliResult<Self> {
        Ok(CliCommand::Run(data))
    }
    fn yield_help(data: Self::Data) -> CliResult<Self> {
        Ok(CliCommand::Help(data))
    }
}

/*
    cli arg impl: multi command (subcommand)
*/

#[derive(Debug, PartialEq)]
pub enum CliMultiCommand {
    Run(CliCommandData),
    Subcommand(Subcommand),
    Help(CliCommandData),
    SubcommandHelp(Subcommand),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Subcommand {
    base_settings: CliCommandData,
    name: Box<str>,
    settings: CliCommandData,
}

impl Subcommand {
    fn new(base_settings: CliCommandData, name: Box<str>, settings: CliCommandData) -> Self {
        Self {
            base_settings,
            name,
            settings,
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn base_settings(&self) -> &CliCommandData {
        &self.base_settings
    }
    pub fn base_settings_mut(&mut self) -> &mut CliCommandData {
        &mut self.base_settings
    }
    pub fn settings(&self) -> &CliCommandData {
        &self.settings
    }
    pub fn settings_mut(&mut self) -> &mut CliCommandData {
        &mut self.settings
    }
}

impl CliArgsDecode for CliMultiCommand {
    type Data = CliCommandData;
    fn initialize<const SWITCH: bool>(iter: &mut impl Iterator<Item = impl ArgItem>) -> Self::Data {
        <CliCommand>::initialize::<SWITCH>(iter)
    }
    fn push_flag(data: &mut Self::Data, flag: Box<str>) -> CliResult<()> {
        <CliCommand>::push_flag(data, flag)
    }
    fn push_option(
        data: &mut Self::Data,
        option_name: Box<str>,
        option_value: Box<str>,
    ) -> CliResult<()> {
        <CliCommand>::push_option(data, option_name, option_value)
    }
    fn yield_command(data: Self::Data) -> CliResult<Self> {
        Ok(Self::Run(data))
    }
    fn yield_help(data: Self::Data) -> CliResult<Self> {
        Ok(Self::Help(data))
    }
    fn yield_subcommand(
        data: Self::Data,
        subcommand: Box<str>,
        args: impl IntoIterator<Item = impl ArgItem>,
    ) -> CliResult<Self> {
        let subcommand_args = decode_args::<CliCommand, false>(args)?;
        match subcommand_args {
            CliCommand::Run(subcommand_data) => Ok(CliMultiCommand::Subcommand(Subcommand::new(
                data,
                subcommand,
                subcommand_data,
            ))),
            CliCommand::Help(subcommand_data) => Ok(CliMultiCommand::SubcommandHelp(
                Subcommand::new(data, subcommand, subcommand_data),
            )),
        }
    }
}

/*
    tests
*/

#[test]
fn command() {
    let cli = CliCommand::parse([
        "skyd",
        "--verify-cluster-seed-membership",
        "--auth-root-password",
        "mypassword12345678",
        "--tls-only",
        "--auth-plugin=pwd",
    ])
    .unwrap();
    assert_eq!(
        cli,
        CliCommand::Run(CliCommandData {
            options: [
                ("auth-root-password", "mypassword12345678"),
                ("auth-plugin", "pwd")
            ]
            .into_iter()
            .map(|(x, y)| (x.to_owned().into_boxed_str(), y.to_owned().into_boxed_str()))
            .collect(),
            flags: ["tls-only", "verify-cluster-seed-membership"]
                .into_iter()
                .map(|f| f.to_owned().into_boxed_str())
                .collect()
        })
    )
}

#[test]
fn subcommand() {
    let cli_input = [
        "skyd",
        "--verify-cluster-membership",
        "--compat-driver=v1",
        "restore",
        "--driver=v2",
        "--name",
        "myoldbackup",
        "--allow-different-host",
    ];
    let expected_subcommand = Subcommand::new(
        CliCommandData {
            options: [("compat-driver", "v1")]
                .into_iter()
                .map(|(x, y)| (x.to_owned().into_boxed_str(), y.to_owned().into_boxed_str()))
                .collect(),
            flags: ["verify-cluster-membership"]
                .into_iter()
                .map(|f| f.to_owned().into_boxed_str())
                .collect(),
        },
        "restore".to_owned().into_boxed_str(),
        CliCommandData {
            options: [("driver", "v2"), ("name", "myoldbackup")]
                .into_iter()
                .map(|(x, y)| (x.to_owned().into_boxed_str(), y.to_owned().into_boxed_str()))
                .collect(),
            flags: ["allow-different-host"]
                .into_iter()
                .map(|f| f.to_owned().into_boxed_str())
                .collect(),
        },
    );
    assert_eq!(
        CliMultiCommand::parse(cli_input).unwrap(),
        CliMultiCommand::Subcommand(expected_subcommand.clone())
    );
    let cli_input = {
        let mut v = Vec::from(cli_input);
        v.push("-h");
        v
    };
    assert_eq!(
        CliMultiCommand::parse(cli_input).unwrap(),
        CliMultiCommand::SubcommandHelp(expected_subcommand)
    )
}
