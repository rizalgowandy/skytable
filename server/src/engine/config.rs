/*
 * Created on Fri Sep 22 2023
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
    crate::engine::{error::RuntimeResult, fractal},
    core::fmt,
    libsky::cli_utils::{ArgItem, CliMultiCommand, CommandLineArgs, MultipleOptions, SingleOption},
    serde::Deserialize,
    std::{collections::HashMap, fs},
};

/*
    misc
*/

pub type ParsedRawArgs = std::collections::HashMap<String, Vec<String>>;
pub const ROOT_PASSWORD_MIN_LEN: usize = 16;

#[derive(Debug, PartialEq)]
pub struct ModifyGuard<T> {
    val: T,
    modified: bool,
}

impl<T> ModifyGuard<T> {
    pub const fn new(val: T) -> Self {
        Self {
            val,
            modified: false,
        }
    }
}

impl<T> core::ops::Deref for ModifyGuard<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

impl<T> core::ops::DerefMut for ModifyGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.modified = true;
        &mut self.val
    }
}

/*
    configuration
*/

#[derive(Debug, PartialEq)]
/// The final configuration that can be used to start up all services
pub struct Configuration {
    pub endpoints: ConfigEndpoint,
    pub mode: ConfigMode,
    pub system: ConfigSystem,
    pub auth: ConfigAuth,
}

impl Configuration {
    #[cfg(test)]
    pub fn new(
        endpoints: ConfigEndpoint,
        mode: ConfigMode,
        system: ConfigSystem,
        auth: ConfigAuth,
    ) -> Self {
        Self {
            endpoints,
            mode,
            system,
            auth,
        }
    }
    const DEFAULT_HOST: &'static str = "127.0.0.1";
    const DEFAULT_PORT_TCP: u16 = 2003;
    pub fn default_dev_mode(auth: DecodedAuth) -> Self {
        Self {
            endpoints: ConfigEndpoint::Insecure(ConfigEndpointTcp {
                host: Self::DEFAULT_HOST.to_owned(),
                port: Self::DEFAULT_PORT_TCP,
            }),
            mode: ConfigMode::Dev,
            system: ConfigSystem::new(fractal::GENERAL_EXECUTOR_WINDOW),
            auth: ConfigAuth::new(auth.plugin, auth.root_pass),
        }
    }
}

// endpoint config

#[derive(Debug, PartialEq)]
/// Endpoint configuration (TCP/TLS/TCP+TLS)
pub enum ConfigEndpoint {
    Insecure(ConfigEndpointTcp),
    Secure(ConfigEndpointTls),
    Multi(ConfigEndpointTcp, ConfigEndpointTls),
}

#[derive(Debug, PartialEq, Clone)]
/// TCP endpoint configuration
pub struct ConfigEndpointTcp {
    host: String,
    port: u16,
}

impl ConfigEndpointTcp {
    #[cfg(test)]
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
    pub fn host(&self) -> &str {
        self.host.as_ref()
    }
    pub fn port(&self) -> u16 {
        self.port
    }
}

#[derive(Debug, PartialEq)]
/// TLS endpoint configuration
pub struct ConfigEndpointTls {
    pub tcp: ConfigEndpointTcp,
    cert: String,
    private_key: String,
    pkey_pass: String,
}

impl ConfigEndpointTls {
    #[cfg(test)]
    pub fn new(
        tcp: ConfigEndpointTcp,
        cert: String,
        private_key: String,
        pkey_pass: String,
    ) -> Self {
        Self {
            tcp,
            cert,
            private_key,
            pkey_pass,
        }
    }
    pub fn tcp(&self) -> &ConfigEndpointTcp {
        &self.tcp
    }
    pub fn cert(&self) -> &str {
        self.cert.as_ref()
    }
    pub fn private_key(&self) -> &str {
        self.private_key.as_ref()
    }
    pub fn pkey_pass(&self) -> &str {
        self.pkey_pass.as_ref()
    }
}

/*
    config mode
*/

#[derive(Debug, PartialEq, Deserialize, Clone, Copy)]
/// The configuration mode
pub enum ConfigMode {
    /// In [`ConfigMode::Dev`] we're allowed to be more relaxed with settings
    #[serde(rename = "dev")]
    Dev,
    /// In [`ConfigMode::Prod`] we're more stringent with settings
    #[serde(rename = "prod")]
    Prod,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BackupType {
    Direct,
}

#[derive(Debug, PartialEq)]
pub struct BackupSettings {
    pub to: String,
    pub from: Option<String>,
    pub kind: BackupType,
    pub description: Option<String>,
    pub allow_dirty: bool,
}

impl BackupSettings {
    fn new(
        to: String,
        from: Option<String>,
        kind: BackupType,
        description: Option<String>,
        allow_dirty: bool,
    ) -> Self {
        Self {
            to,
            from,
            kind,
            description,
            allow_dirty,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct RestoreSettings {
    pub from: String,
    pub to: Option<String>,
    pub flag_allow_incompatible: bool,
    pub flag_allow_different_host: bool,
    pub flag_allow_invalid_date: bool,
    pub flag_delete_on_restore_completion: bool,
    pub flag_skip_compatibility_check: bool,
}

impl RestoreSettings {
    fn new(
        from: String,
        to: Option<String>,
        flag_allow_incompatible: bool,
        flag_allow_different_host: bool,
        flag_allow_invalid_date: bool,
        flag_delete_on_restore_completion: bool,
        flag_skip_compatibility_check: bool,
    ) -> Self {
        Self {
            from,
            to,
            flag_allow_incompatible,
            flag_allow_different_host,
            flag_allow_invalid_date,
            flag_delete_on_restore_completion,
            flag_skip_compatibility_check,
        }
    }
}

/*
    config system
*/

#[derive(Debug, PartialEq)]
/// System configuration settings
pub struct ConfigSystem {
    /// time window in seconds for the reliability system to kick-in automatically
    pub reliability_system_window: u64,
}

impl ConfigSystem {
    pub fn new(reliability_system_window: u64) -> Self {
        Self {
            reliability_system_window,
        }
    }
}

/*
    config auth
*/

#[derive(Debug, PartialEq, Deserialize, Clone, Copy)]
pub enum AuthDriver {
    #[serde(rename = "pwd")]
    Pwd,
}

#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct ConfigAuth {
    pub plugin: AuthDriver,
    pub root_key: String,
}

impl ConfigAuth {
    pub fn new(plugin: AuthDriver, root_key: String) -> Self {
        Self { plugin, root_key }
    }
}

/**
    decoded configuration
    ---
    the "raw" configuration that we got from the user. not validated
*/
#[derive(Debug, PartialEq, Deserialize)]
pub struct DecodedConfiguration {
    system: Option<DecodedSystemConfig>,
    endpoints: Option<DecodedEPConfig>,
    auth: Option<DecodedAuth>,
}

impl Default for DecodedConfiguration {
    fn default() -> Self {
        Self {
            system: Default::default(),
            endpoints: Default::default(),
            auth: None,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct DecodedAuth {
    plugin: AuthDriver,
    root_pass: String,
}

#[derive(Debug, PartialEq, Deserialize)]
/// Decoded system configuration
pub struct DecodedSystemConfig {
    mode: Option<ConfigMode>,
    rs_window: Option<u64>,
}

#[derive(Debug, PartialEq, Deserialize)]
/// Decoded endpoint configuration
pub struct DecodedEPConfig {
    secure: Option<DecodedEPSecureConfig>,
    insecure: Option<DecodedEPInsecureConfig>,
}

#[derive(Debug, PartialEq, Deserialize)]
/// Decoded secure port configuration
pub struct DecodedEPSecureConfig {
    host: String,
    port: u16,
    cert: String,
    private_key: String,
    pkey_passphrase: String,
}

#[derive(Debug, PartialEq, Deserialize)]
/// Decoded insecure port configuration
pub struct DecodedEPInsecureConfig {
    host: String,
    port: u16,
}

impl DecodedEPInsecureConfig {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_owned(),
            port,
        }
    }
}

/*
    errors and misc
*/

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// A configuration error (with an optional error origin source)
pub struct ConfigError {
    source: Option<ConfigSource>,
    kind: ConfigErrorKind,
}

impl From<libsky::cli_utils::CliArgsError> for ConfigError {
    fn from(err: libsky::cli_utils::CliArgsError) -> Self {
        Self::with_src(
            ConfigSource::Cli,
            ConfigErrorKind::ErrorString(err.to_string()),
        )
    }
}

impl ConfigError {
    /// Init config error
    fn _new(source: Option<ConfigSource>, kind: ConfigErrorKind) -> Self {
        Self { kind, source }
    }
    /// New config error with no source
    fn new(kind: ConfigErrorKind) -> Self {
        Self::_new(None, kind)
    }
    /// New config error with the given source
    fn with_src(source: ConfigSource, kind: ConfigErrorKind) -> Self {
        Self::_new(Some(source), kind)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(src) => write!(f, "config error in {}: ", src.as_str())?,
            None => {}
        }
        match &self.kind {
            ConfigErrorKind::Conflict => write!(
                f,
                "conflicting settings. please choose either CLI or ENV or configuration file"
            ),
            ConfigErrorKind::ErrorString(e) => write!(f, "{e}"),
        }
    }
}

#[derive(Debug, PartialEq)]
/// The configuration source
pub enum ConfigSource {
    /// Command-line
    Cli,
    /// Environment variabels
    Env,
    /// Configuration file
    File,
}

impl ConfigSource {
    fn as_str(&self) -> &'static str {
        match self {
            ConfigSource::Cli => "command-line arguments",
            ConfigSource::Env => "ENV",
            ConfigSource::File => "config file",
        }
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
/// Type of configuration error
pub enum ConfigErrorKind {
    /// Conflict between different setting modes (more than one of CLI/ENV/FILE was provided)
    Conflict,
    /// A custom error output
    ErrorString(String),
}

/// A configuration source implementation
pub(super) trait ConfigurationSource {
    const KEY_AUTH_DRIVER: &'static str;
    const KEY_AUTH_ROOT_PASSWORD: &'static str;
    const KEY_TLS_CERT: &'static str;
    const KEY_TLS_KEY: &'static str;
    const KEY_TLS_PKEY_PASS: &'static str;
    const KEY_ENDPOINTS: &'static str;
    const KEY_RUN_MODE: &'static str;
    const KEY_SERVICE_WINDOW: &'static str;
    const SOURCE: ConfigSource;
    /// Formats an error `Invalid value for {key}`
    fn err_invalid_value_for(key: &str) -> ConfigError {
        let msg;
        if Self::SOURCE == ConfigSource::Cli {
            msg = format!("invalid value for `--{key}`");
        } else {
            msg = format!("invalid value for {key}");
        }
        ConfigError::with_src(Self::SOURCE, ConfigErrorKind::ErrorString(msg))
    }
    /// Formats an error `Too many values for {key}`
    fn err_too_many_values_for(key: &str) -> ConfigError {
        let msg;
        if Self::SOURCE == ConfigSource::Cli {
            msg = format!("too many values for `--{key}`");
        } else {
            msg = format!("too many values for {key}");
        }
        ConfigError::with_src(Self::SOURCE, ConfigErrorKind::ErrorString(msg))
    }
    /// Formats the custom error directly
    fn custom_err(error: String) -> ConfigError {
        ConfigError::with_src(Self::SOURCE, ConfigErrorKind::ErrorString(error))
    }
}

/// Check if there are any duplicate values
fn argck_duplicate_values<CS: ConfigurationSource>(
    v: &[impl AsRef<str>],
    key: &'static str,
) -> RuntimeResult<()> {
    if v.len() != 1 {
        return Err(CS::err_too_many_values_for(key).into());
    }
    Ok(())
}

/*
    decode helpers
*/

/// Protocol to be used by a given endpoint
enum ConnectionProtocol {
    Tcp,
    Tls,
}

/// Parse an endpoint (`protocol@host:port`)
fn parse_endpoint(source: ConfigSource, s: &str) -> RuntimeResult<(ConnectionProtocol, &str, u16)> {
    let err = || {
        Err(ConfigError::with_src(
            source,
            ConfigErrorKind::ErrorString(format!(
                "invalid endpoint syntax. should be `protocol@hostname:port`"
            )),
        )
        .into())
    };
    let x = s.split("@").collect::<Vec<&str>>();
    if x.len() != 2 {
        return err();
    }
    let [protocol, hostport] = [x[0], x[1]];
    let hostport = hostport.split(":").collect::<Vec<&str>>();
    if hostport.len() != 2 {
        return err();
    }
    let [host, port] = [hostport[0], hostport[1]];
    let Ok(port) = port.parse::<u16>() else {
        return err();
    };
    let protocol = match protocol {
        "tcp" => ConnectionProtocol::Tcp,
        "tls" => ConnectionProtocol::Tls,
        _ => return err(),
    };
    Ok((protocol, host, port))
}

/// Decode a TLS endpoint (read in cert and private key)
fn decode_tls_ep(
    cert_path: &str,
    key_path: &str,
    pkey_pass: &str,
    host: &str,
    port: u16,
) -> RuntimeResult<DecodedEPSecureConfig> {
    super::fractal::context::set_dmsg("loading TLS configuration from disk");
    let tls_key = fs::read_to_string(key_path)?;
    let tls_cert = fs::read_to_string(cert_path)?;
    let tls_priv_key_passphrase = fs::read_to_string(pkey_pass)?;
    Ok(DecodedEPSecureConfig {
        host: host.into(),
        port,
        cert: tls_cert,
        private_key: tls_key,
        pkey_passphrase: tls_priv_key_passphrase,
    })
}

/// Helper for decoding a TLS endpoint (we read in the cert and private key)
fn arg_decode_tls_endpoint<CS: ConfigurationSource>(
    args: &mut ParsedRawArgs,
    host: &str,
    port: u16,
) -> RuntimeResult<DecodedEPSecureConfig> {
    let _cert = args.remove(CS::KEY_TLS_CERT);
    let _key = args.remove(CS::KEY_TLS_KEY);
    let _passphrase = args.remove(CS::KEY_TLS_PKEY_PASS);
    let (tls_cert, tls_key, tls_passphrase) = match (_cert, _key, _passphrase) {
        (Some(cert), Some(key), Some(pass)) => (cert, key, pass),
        _ => {
            return Err(ConfigError::with_src(
                ConfigSource::Cli,
                ConfigErrorKind::ErrorString(format!(
                    "must supply values for `{}`, `{}` and `{}` when using TLS",
                    CS::KEY_TLS_CERT,
                    CS::KEY_TLS_KEY,
                    CS::KEY_TLS_PKEY_PASS,
                )),
            )
            .into());
        }
    };
    argck_duplicate_values::<CS>(&tls_cert, CS::KEY_TLS_CERT)?;
    argck_duplicate_values::<CS>(&tls_key, CS::KEY_TLS_KEY)?;
    argck_duplicate_values::<CS>(&tls_passphrase, CS::KEY_TLS_PKEY_PASS)?;
    Ok(decode_tls_ep(
        &tls_cert[0],
        &tls_key[0],
        &tls_passphrase[0],
        host,
        port,
    )?)
}

/*
    decode options
*/

fn arg_decode_auth<CS: ConfigurationSource>(
    src_args: &mut ParsedRawArgs,
    config: &mut ModifyGuard<DecodedConfiguration>,
) -> RuntimeResult<()> {
    let auth_driver = src_args.remove(CS::KEY_AUTH_DRIVER);
    let Some(mut root_key) = src_args.remove(CS::KEY_AUTH_ROOT_PASSWORD) else {
        return Err(ConfigError::with_src(
            CS::SOURCE,
            ConfigErrorKind::ErrorString(format!(
                "to enable password auth, you must provide a value for '{}'",
                CS::KEY_AUTH_ROOT_PASSWORD,
            )),
        )
        .into());
    };
    if let Some(ref adrv) = auth_driver {
        argck_duplicate_values::<CS>(&adrv, CS::KEY_AUTH_DRIVER)?;
    }
    argck_duplicate_values::<CS>(&root_key, CS::KEY_AUTH_DRIVER)?;
    let auth_plugin = match auth_driver.as_ref().map(|v| v[0].as_ref()) {
        Some("pwd") | None => AuthDriver::Pwd,
        _ => return Err(CS::err_invalid_value_for(CS::KEY_AUTH_DRIVER).into()),
    };
    config.auth = Some(DecodedAuth {
        plugin: auth_plugin,
        root_pass: root_key.remove(0),
    });
    Ok(())
}

/// Decode the endpoints (`protocol@host:port`)
fn arg_decode_endpoints<CS: ConfigurationSource>(
    args: &mut ParsedRawArgs,
    config: &mut ModifyGuard<DecodedConfiguration>,
) -> RuntimeResult<()> {
    let mut insecure = None;
    let mut secure = None;
    let Some(endpoints) = args.remove(CS::KEY_ENDPOINTS) else {
        return Ok(());
    };
    if endpoints.len() > 2 {
        return Err(CS::err_too_many_values_for(CS::KEY_ENDPOINTS).into());
    }
    for ep in endpoints {
        let (proto, host, port) = parse_endpoint(CS::SOURCE, &ep)?;
        match proto {
            ConnectionProtocol::Tcp if insecure.is_none() => {
                insecure = Some(DecodedEPInsecureConfig::new(host, port));
            }
            ConnectionProtocol::Tls if secure.is_none() => {
                secure = Some(arg_decode_tls_endpoint::<CS>(args, host, port)?);
            }
            _ => {
                return Err(CS::custom_err(format!(
                    "duplicate endpoints specified in `{}`",
                    CS::KEY_ENDPOINTS
                ))
                .into());
            }
        }
    }
    if insecure.is_some() | secure.is_some() {
        config.endpoints = Some(DecodedEPConfig { secure, insecure });
    }
    Ok(())
}

/// Decode the run mode:
/// - Dev OR
/// - Prod
fn arg_decode_mode<CS: ConfigurationSource>(
    mode: &[String],
    config: &mut ModifyGuard<DecodedConfiguration>,
) -> RuntimeResult<()> {
    argck_duplicate_values::<CS>(&mode, CS::KEY_RUN_MODE)?;
    let mode = match mode[0].as_str() {
        "dev" => ConfigMode::Dev,
        "prod" => ConfigMode::Prod,
        _ => return Err(CS::err_invalid_value_for(CS::KEY_RUN_MODE).into()),
    };
    match config.system.as_mut() {
        Some(s) => s.mode = Some(mode),
        None => {
            config.system = Some(DecodedSystemConfig {
                mode: Some(mode),
                rs_window: None,
            })
        }
    }
    Ok(())
}

/// Decode the service time window
fn arg_decode_rs_window<CS: ConfigurationSource>(
    mode: &[String],
    config: &mut ModifyGuard<DecodedConfiguration>,
) -> RuntimeResult<()> {
    argck_duplicate_values::<CS>(&mode, CS::KEY_SERVICE_WINDOW)?;
    match mode[0].parse::<u64>() {
        Ok(n) => match config.system.as_mut() {
            Some(sys) => sys.rs_window = Some(n),
            None => {
                config.system = Some(DecodedSystemConfig {
                    mode: None,
                    rs_window: Some(n),
                })
            }
        },
        Err(_) => return Err(CS::err_invalid_value_for(CS::KEY_SERVICE_WINDOW).into()),
    }
    Ok(())
}

/*
    CLI args process
*/

/// CLI help message
pub(super) const TXT_HELP: &str = include_str!(concat!(env!("OUT_DIR"), "/skyd-help"));
pub(super) const TXT_HELP_REPAIR: &str = include_str!(concat!(env!("OUT_DIR"), "/skyd-repair"));
pub(super) const TXT_HELP_COMPACT: &str = include_str!(concat!(env!("OUT_DIR"), "/skyd-compact"));
pub(super) const TXT_HELP_BACKUP: &str = include_str!(concat!(env!("OUT_DIR"), "/skyd-backup"));
pub(super) const TXT_HELP_RESTORE: &str = include_str!(concat!(env!("OUT_DIR"), "/skyd-restore"));

#[derive(Debug, PartialEq)]
/// Return from parsing CLI configuration
pub enum CLIConfigParseReturn<T> {
    /// No changes
    Default,
    /// Output help menu
    Help(String),
    /// Output version
    Version,
    /// We yielded a config
    YieldedConfig(T),
    /// a repair was requested
    Repair,
    /// a compact operation was requested
    Compact,
    /// a backup operation was requested
    Backup(BackupSettings),
    /// a restore operation was requested
    Restore(RestoreSettings),
}

impl<T> CLIConfigParseReturn<T> {
    #[cfg(test)]
    pub fn into_config(self) -> T {
        match self {
            Self::YieldedConfig(yc) => yc,
            _ => panic!(),
        }
    }
}

pub fn parse_cli_args<'a, T: ArgItem>(
    src: impl Iterator<Item = T>,
) -> RuntimeResult<CLIConfigParseReturn<ParsedRawArgs>> {
    Ok(
        match libsky::cli_utils::CliMultiCommand::<MultipleOptions, SingleOption>::parse(src)? {
            CliMultiCommand::Run(data) => {
                let opts = data.into_options_only()?;
                if opts.is_empty() {
                    CLIConfigParseReturn::Default
                } else {
                    CLIConfigParseReturn::YieldedConfig(opts)
                }
            }
            CliMultiCommand::Help(_) => CLIConfigParseReturn::Help(TXT_HELP.to_string()),
            CliMultiCommand::SubcommandHelp(_, subcommand) => match subcommand.name() {
                "repair" => CLIConfigParseReturn::Help(TXT_HELP_REPAIR.to_owned()),
                "compact" => CLIConfigParseReturn::Help(TXT_HELP_COMPACT.to_owned()),
                "backup" => CLIConfigParseReturn::Help(TXT_HELP_BACKUP.to_owned()),
                "restore" => CLIConfigParseReturn::Help(TXT_HELP_RESTORE.to_owned()),
                _ => {
                    return Err(ConfigError::with_src(
                        ConfigSource::Cli,
                        ConfigErrorKind::ErrorString(format!(
                            "unknown subcommand {}",
                            subcommand.name()
                        )),
                    )
                    .into())
                }
            },
            CliMultiCommand::Version(_) | CliMultiCommand::SubcommandVersion(_, _) => {
                CLIConfigParseReturn::Version
            }
            CliMultiCommand::Subcommand(command, subcommand) => {
                command.ensure_empty()?;
                match subcommand.name() {
                    "repair" => {
                        subcommand.settings().ensure_empty()?;
                        CLIConfigParseReturn::Repair
                    }
                    "compact" => {
                        subcommand.settings().ensure_empty()?;
                        CLIConfigParseReturn::Compact
                    }
                    "backup" => {
                        let mut subcommand = subcommand;
                        let backup_to = subcommand.settings_mut().option("to")?;
                        let backup_from = subcommand.settings_mut().take_option("from")?;
                        let backup_kind = match subcommand.settings_mut().option("type")?.as_ref() {
                            "direct" => BackupType::Direct,
                            backup_scheme => {
                                return Err(ConfigError::with_src(
                                    ConfigSource::Cli,
                                    ConfigErrorKind::ErrorString(format!(
                                        "unknown backup scheme `{backup_scheme}`"
                                    )),
                                )
                                .into())
                            }
                        };
                        let backup_flag_allow_dirty =
                            subcommand.settings_mut().take_flag("allow-dirty")?;
                        let backup_description =
                            subcommand.settings_mut().take_option("description")?;
                        subcommand.settings().ensure_empty()?;
                        CLIConfigParseReturn::Backup(BackupSettings::new(
                            backup_to,
                            backup_from,
                            backup_kind,
                            backup_description,
                            backup_flag_allow_dirty,
                        ))
                    }
                    "restore" => {
                        let mut subcommand = subcommand;
                        let restore_from = subcommand.settings_mut().option("from")?;
                        let restore_to = subcommand.settings_mut().take_option("to")?;
                        let flag_allow_incompatible =
                            subcommand.settings_mut().take_flag("allow-incompatible")?;
                        let flag_allow_different_host = subcommand
                            .settings_mut()
                            .take_flag("allow-different-host")?;
                        let flag_allow_invalid_date =
                            subcommand.settings_mut().take_flag("allow-invalid-date")?;
                        let flag_delete_on_restore =
                            subcommand.settings_mut().take_flag("delete-on-restore")?;
                        let flag_skip_compatibility_check = subcommand
                            .settings_mut()
                            .take_flag("skip-compatibility-check")?;
                        subcommand.settings().ensure_empty()?;
                        CLIConfigParseReturn::Restore(RestoreSettings::new(
                            restore_from,
                            restore_to,
                            flag_allow_incompatible,
                            flag_allow_different_host,
                            flag_allow_invalid_date,
                            flag_delete_on_restore,
                            flag_skip_compatibility_check,
                        ))
                    }
                    _ => {
                        return Err(ConfigError::with_src(
                            ConfigSource::Cli,
                            ConfigErrorKind::ErrorString(format!(
                                "unknown subcommand {}",
                                subcommand.name()
                            )),
                        )
                        .into())
                    }
                }
            }
        },
    )
}

/*
    env args process
*/

/// Parse environment variables
pub fn parse_env_args() -> RuntimeResult<Option<ParsedRawArgs>> {
    const KEYS: [&str; 8] = [
        CSEnvArgs::KEY_AUTH_DRIVER,
        CSEnvArgs::KEY_AUTH_ROOT_PASSWORD,
        CSEnvArgs::KEY_ENDPOINTS,
        CSEnvArgs::KEY_RUN_MODE,
        CSEnvArgs::KEY_SERVICE_WINDOW,
        CSEnvArgs::KEY_TLS_CERT,
        CSEnvArgs::KEY_TLS_KEY,
        CSEnvArgs::KEY_TLS_PKEY_PASS,
    ];
    let mut ret = HashMap::new();
    for key in KEYS {
        let var = match get_var_from_store(key) {
            Ok(v) => v,
            Err(e) => match e {
                std::env::VarError::NotPresent => continue,
                std::env::VarError::NotUnicode(_) => {
                    return Err(ConfigError::with_src(
                        ConfigSource::Env,
                        ConfigErrorKind::ErrorString(format!("invalid value for `{key}`")),
                    )
                    .into())
                }
            },
        };
        let splits: Vec<_> = var.split(",").map(ToString::to_string).collect();
        ret.insert(key.into(), splits);
    }
    if ret.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ret))
    }
}

/*
    apply config changes
*/

/// Apply the configuration changes to the given mutable config
fn apply_config_changes<CS: ConfigurationSource>(
    args: &mut ParsedRawArgs,
) -> RuntimeResult<ModifyGuard<DecodedConfiguration>> {
    let mut config = ModifyGuard::new(DecodedConfiguration::default());
    enum DecodeKind {
        Simple {
            key: &'static str,
            f: fn(&[String], &mut ModifyGuard<DecodedConfiguration>) -> RuntimeResult<()>,
        },
        Complex {
            f: fn(&mut ParsedRawArgs, &mut ModifyGuard<DecodedConfiguration>) -> RuntimeResult<()>,
        },
    }
    let decode_tasks = [
        // auth
        DecodeKind::Complex {
            f: arg_decode_auth::<CS>,
        },
        // mode
        DecodeKind::Simple {
            key: CS::KEY_RUN_MODE,
            f: arg_decode_mode::<CS>,
        },
        // service time window
        DecodeKind::Simple {
            key: CS::KEY_SERVICE_WINDOW,
            f: arg_decode_rs_window::<CS>,
        },
        // endpoints
        DecodeKind::Complex {
            f: arg_decode_endpoints::<CS>,
        },
    ];
    for task in decode_tasks {
        match task {
            DecodeKind::Simple { key, f } => match args.get(key) {
                Some(values_for_arg) => {
                    (f)(&values_for_arg, &mut config)?;
                    args.remove(key);
                }
                None => {}
            },
            DecodeKind::Complex { f } => (f)(args, &mut config)?,
        }
    }
    if !args.is_empty() {
        Err(ConfigError::with_src(
            CS::SOURCE,
            ConfigErrorKind::ErrorString("found unknown arguments".to_string()),
        )
        .into())
    } else {
        Ok(config)
    }
}

/*
    config source impls
*/

pub struct CSCommandLine;
impl CSCommandLine {
    const ARG_CONFIG_FILE: &'static str = "config";
}
impl ConfigurationSource for CSCommandLine {
    const KEY_AUTH_DRIVER: &'static str = "auth-plugin";
    const KEY_AUTH_ROOT_PASSWORD: &'static str = "auth-root-password";
    const KEY_TLS_CERT: &'static str = "tlscert";
    const KEY_TLS_KEY: &'static str = "tlskey";
    const KEY_TLS_PKEY_PASS: &'static str = "tls-passphrase";
    const KEY_ENDPOINTS: &'static str = "endpoint";
    const KEY_RUN_MODE: &'static str = "mode";
    const KEY_SERVICE_WINDOW: &'static str = "service-window";
    const SOURCE: ConfigSource = ConfigSource::Cli;
}

pub struct CSEnvArgs;
impl ConfigurationSource for CSEnvArgs {
    const KEY_AUTH_DRIVER: &'static str = "SKYDB_AUTH_PLUGIN";
    const KEY_AUTH_ROOT_PASSWORD: &'static str = "SKYDB_AUTH_ROOT_PASSWORD";
    const KEY_TLS_CERT: &'static str = "SKYDB_TLS_CERT";
    const KEY_TLS_KEY: &'static str = "SKYDB_TLS_KEY";
    const KEY_TLS_PKEY_PASS: &'static str = "SKYDB_TLS_PRIVATE_KEY_PASSWORD";
    const KEY_ENDPOINTS: &'static str = "SKYDB_ENDPOINTS";
    const KEY_RUN_MODE: &'static str = "SKYDB_RUN_MODE";
    const KEY_SERVICE_WINDOW: &'static str = "SKYDB_SERVICE_WINDOW";
    const SOURCE: ConfigSource = ConfigSource::Env;
}

pub struct CSConfigFile;
impl ConfigurationSource for CSConfigFile {
    const KEY_AUTH_DRIVER: &'static str = "auth.plugin";
    const KEY_AUTH_ROOT_PASSWORD: &'static str = "auth.root_password";
    const KEY_TLS_CERT: &'static str = "endpoints.secure.cert";
    const KEY_TLS_KEY: &'static str = "endpoints.secure.key";
    const KEY_TLS_PKEY_PASS: &'static str = "endpoints.secure.pkey_passphrase";
    const KEY_ENDPOINTS: &'static str = "endpoints";
    const KEY_RUN_MODE: &'static str = "system.mode";
    const KEY_SERVICE_WINDOW: &'static str = "system.service_window";
    const SOURCE: ConfigSource = ConfigSource::File;
}

/*
    validate configuration
*/

macro_rules! if_some {
    ($target:expr => $then:expr) => {
        if let Some(x) = $target {
            $then(x);
        }
    };
}

macro_rules! err_if {
    ($(if $cond:expr => $error:expr),* $(,)?) => {
        $(if $cond { return Err($error) })*
    }
}

/// Validate the configuration, and prepare the final configuration
fn validate_configuration<CS: ConfigurationSource>(
    DecodedConfiguration {
        system,
        endpoints,
        auth,
    }: DecodedConfiguration,
) -> RuntimeResult<Configuration> {
    let Some(auth) = auth else {
        return Err(ConfigError::with_src(
            CS::SOURCE,
            ConfigErrorKind::ErrorString(format!(
                "root account must be configured with {}",
                CS::KEY_AUTH_ROOT_PASSWORD
            )),
        )
        .into());
    };
    // initialize our default configuration
    let mut config = Configuration::default_dev_mode(auth);
    // mutate
    if_some!(
        system => |system: DecodedSystemConfig| {
            if_some!(system.mode => |mode| config.mode = mode);
            if_some!(system.rs_window => |window| config.system.reliability_system_window = window);
        }
    );
    if_some!(
        endpoints => |ep: DecodedEPConfig| {
            let has_insecure = ep.insecure.is_some();
            if_some!(ep.insecure => |insecure: DecodedEPInsecureConfig| {
                config.endpoints = ConfigEndpoint::Insecure(ConfigEndpointTcp { host: insecure.host, port: insecure.port });
            });
            if_some!(ep.secure => |secure: DecodedEPSecureConfig| {
                let secure_ep = ConfigEndpointTls {
                    tcp: ConfigEndpointTcp {
                        host: secure.host,
                        port: secure.port,
                    },
                    cert: secure.cert,
                    private_key: secure.private_key,
                    pkey_pass: secure.pkey_passphrase,
                };
                match &config.endpoints {
                    ConfigEndpoint::Insecure(is) => if has_insecure {
                        // an insecure EP was defined by the user, so set to multi
                        config.endpoints = ConfigEndpoint::Multi(is.clone(), secure_ep)
                    } else {
                        // only secure EP was defined by the user
                        config.endpoints = ConfigEndpoint::Secure(secure_ep);
                    },
                    _ => unreachable!()
                }
            })
        }
    );
    // now check a few things
    err_if!(
        if config.system.reliability_system_window == 0 => ConfigError::with_src(
            CS::SOURCE,
            ConfigErrorKind::ErrorString("invalid value for service window. must be nonzero".to_string()),
        ).into(),
        if config.auth.root_key.len() < ROOT_PASSWORD_MIN_LEN => ConfigError::with_src(
            CS::SOURCE,
            ConfigErrorKind::ErrorString("the root password must have at least 16 characters".to_string()),
        ).into(),
    );
    Ok(config)
}

/*
    actual configuration check and exec
*/

/// The return from parsing a configuration file
#[derive(Debug, PartialEq)]
pub enum ConfigReturn {
    /// Don't need to do anything. We've output a message and we're good to exit
    HelpMessage(String),
    /// A configuration that we have fully validated was provided
    Config(Configuration),
    Repair,
    Compact,
    Backup(BackupSettings),
    Restore(RestoreSettings),
}

impl ConfigReturn {
    #[cfg(test)]
    pub fn into_config(self) -> Configuration {
        match self {
            Self::Config(c) => c,
            _ => panic!(),
        }
    }
}

/// Apply the changes and validate the configuration
pub(super) fn apply_and_validate<CS: ConfigurationSource>(
    mut args: ParsedRawArgs,
) -> RuntimeResult<ConfigReturn> {
    let cfg = apply_config_changes::<CS>(&mut args)?;
    validate_configuration::<CS>(cfg.val).map(ConfigReturn::Config)
}

/*
    just some test hacks
*/

#[cfg(test)]
local! {
    static CLI_SRC: Option<Vec<String>> = None;
    static ENV_SRC: Option<HashMap<String, String>> = None;
    static FILE_SRC: Option<String> = None;
}

#[cfg(test)]
pub(super) fn set_cli_src(cli: Vec<String>) {
    local_mut!(CLI_SRC, |args| *args = Some(cli))
}

#[cfg(test)]
pub(super) fn set_env_src(variables: Vec<String>) {
    local_mut!(ENV_SRC, |env| {
        *env = Some(
            variables
                .into_iter()
                .map(|var| {
                    var.split("=")
                        .map(ToString::to_string)
                        .collect::<Vec<String>>()
                })
                .map(|mut vars| (vars.remove(0), vars.remove(0)))
                .collect(),
        );
    });
}

#[cfg(test)]
pub(super) fn set_file_src(new_src: &str) {
    local_mut!(FILE_SRC, |src| *src = Some(new_src.to_string()))
}

fn get_file_from_store(filename: &str) -> RuntimeResult<String> {
    let _f = filename;
    let f;
    #[cfg(test)]
    {
        f = Ok(local_ref!(FILE_SRC, |f| f.clone().unwrap()));
    }
    #[cfg(not(test))]
    {
        super::fractal::context::set_dmsg("loading configuration file from disk");
        f = Ok(fs::read_to_string(filename)?);
    }
    f
}
fn get_var_from_store(name: &str) -> Result<String, std::env::VarError> {
    let var;
    #[cfg(test)]
    {
        var = local_mut!(ENV_SRC, |venv| {
            match venv.as_mut() {
                None => return Err(std::env::VarError::NotPresent),
                Some(env_store) => match env_store.remove(name) {
                    Some(var) => Ok(var),
                    None => Err(std::env::VarError::NotPresent),
                },
            }
        });
    }
    #[cfg(not(test))]
    {
        var = std::env::var(name);
    }
    var
}
fn get_cli_from_store() -> Vec<String> {
    let src;
    #[cfg(test)]
    {
        src = local_mut!(CLI_SRC, core::mem::take).unwrap_or_default();
    }
    #[cfg(not(test))]
    {
        src = std::env::args().collect();
    }
    src
}

/// Check the configuration. We look through:
/// - CLI args
/// - ENV variables
/// - Config file (if any)
pub fn check_configuration() -> RuntimeResult<ConfigReturn> {
    // read in our environment variables
    let env_args = parse_env_args()?;
    // read in our CLI args (since that can tell us whether we need a configuration file)
    let read_cli_args = parse_cli_args(get_cli_from_store().into_iter())?;
    let cli_args = match read_cli_args {
        CLIConfigParseReturn::Default => {
            // no options were provided in the CLI
            None
        }
        CLIConfigParseReturn::Compact => return Ok(ConfigReturn::Compact),
        CLIConfigParseReturn::Help(txt) => return Ok(ConfigReturn::HelpMessage(txt)),
        CLIConfigParseReturn::Version => {
            // just output the version
            return Ok(ConfigReturn::HelpMessage(format!(
                "Skytable Database Server (skyd) v{}",
                libsky::variables::VERSION
            )));
        }
        CLIConfigParseReturn::Repair => return Ok(ConfigReturn::Repair),
        CLIConfigParseReturn::YieldedConfig(cfg) => Some(cfg),
        CLIConfigParseReturn::Backup(bkp) => return Ok(ConfigReturn::Backup(bkp)),
        CLIConfigParseReturn::Restore(restore) => return Ok(ConfigReturn::Restore(restore)),
    };
    match cli_args {
        Some(cfg_from_cli) => {
            // we have some CLI args
            match cfg_from_cli.get(CSCommandLine::ARG_CONFIG_FILE) {
                Some(cfg_file) => return check_config_file(&cfg_from_cli, &env_args, cfg_file),
                None => {
                    // no config file; check if there is a conflict with environment args
                    if env_args.is_some() {
                        // as we feared
                        return Err(ConfigError::with_src(
                            ConfigSource::Cli,
                            ConfigErrorKind::Conflict,
                        )
                        .into());
                    }
                    return apply_and_validate::<CSCommandLine>(cfg_from_cli);
                }
            }
        }
        None => {
            // no CLI args; but do we have anything from env?
            match env_args {
                Some(args) => {
                    return apply_and_validate::<CSEnvArgs>(args);
                }
                None => {
                    // no env args or cli args; we're running on default
                    return Err(ConfigError::new(ConfigErrorKind::ErrorString(
                        "no configuration provided".to_string(),
                    ))
                    .into());
                }
            }
        }
    }
}

/// Check the configuration file
fn check_config_file(
    cfg_from_cli: &ParsedRawArgs,
    env_args: &Option<ParsedRawArgs>,
    cfg_file: &Vec<String>,
) -> RuntimeResult<ConfigReturn> {
    if cfg_from_cli.len() == 1 && env_args.is_none() {
        // yes, we only have the config file
        argck_duplicate_values::<CSCommandLine>(&cfg_file, CSCommandLine::ARG_CONFIG_FILE)?;
        // read the config file
        let file = get_file_from_store(&cfg_file[0])?;
        let mut config_from_file: DecodedConfiguration =
            serde_yaml::from_str(&file).map_err(|e| {
                ConfigError::with_src(
                    ConfigSource::File,
                    ConfigErrorKind::ErrorString(format!(
                        "failed to parse YAML config file with error: `{e}`"
                    )),
                )
            })?;
        // read in the TLS certs (if any)
        match config_from_file.endpoints.as_mut() {
            Some(ep) => match ep.secure.as_mut() {
                Some(secure_ep) => {
                    super::fractal::context::set_dmsg("loading TLS configuration from disk");
                    let cert = fs::read_to_string(&secure_ep.cert)?;
                    let private_key = fs::read_to_string(&secure_ep.private_key)?;
                    let private_key_passphrase = fs::read_to_string(&secure_ep.pkey_passphrase)?;
                    secure_ep.cert = cert;
                    secure_ep.private_key = private_key;
                    secure_ep.pkey_passphrase = private_key_passphrase;
                }
                None => {}
            },
            None => {}
        }
        // done here
        return validate_configuration::<CSConfigFile>(config_from_file).map(ConfigReturn::Config);
    } else {
        // so there are more configuration options + a config file? (and maybe even env?)
        return Err(ConfigError::with_src(ConfigSource::Cli, ConfigErrorKind::Conflict).into());
    }
}
