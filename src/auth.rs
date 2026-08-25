//! Cleartext-password auth for the pgwire startup handshake: one shared server password
//! (`DELTAT_PASSWORD`) verified against every connection, optionally overridden per tenant
//! (`DELTAT_TENANT_PASSWORDS`) keyed by the connection's database name.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgwire::api::auth::{
    self, AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::{ClientInfo, PgWireConnectionState};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

pub struct DeltaTAuthSource {
    password: String,
    /// Sanitized tenant name -> that tenant's password. A tenant with an entry accepts only its
    /// own password; the global one no longer opens it.
    tenant_passwords: HashMap<String, String>,
}

impl DeltaTAuthSource {
    pub fn new(password: String) -> Self {
        Self::with_tenant_passwords(password, HashMap::new())
    }

    pub fn with_tenant_passwords(
        password: String,
        tenant_passwords: HashMap<String, String>,
    ) -> Self {
        Self { password, tenant_passwords }
    }
}

/// Parse `DELTAT_TENANT_PASSWORDS`: comma-separated `tenant:password` pairs, split on the first
/// colon so passwords may contain colons (but not commas). Keys are stored sanitized, matching how
/// the tenant manager keys engines. Malformed or duplicate entries are startup errors: silently
/// dropping one would leave a tenant open on the global password.
pub fn parse_tenant_passwords(raw: &str) -> Result<HashMap<String, String>, String> {
    // Errors name the entry by position, never by content: a malformed entry can contain a
    // password, and these messages go to logs.
    let mut map = HashMap::new();
    for (idx, pair) in raw.split(',').enumerate() {
        let entry = idx + 1;
        let (tenant, password) = pair
            .split_once(':')
            .ok_or_else(|| format!("tenant password entry {entry} is not tenant:password"))?;
        if password.is_empty() {
            return Err(format!("tenant password entry {entry} has an empty password"));
        }
        let key = crate::tenant::TenantManager::sanitize(tenant)
            .map_err(|_| format!("tenant password entry {entry} has an empty tenant name"))?;
        if map.insert(key.clone(), password.to_string()).is_some() {
            return Err(format!("duplicate tenant password entry for '{key}'"));
        }
    }
    Ok(map)
}

// Redact the shared password so it can never reach a log line through a derived Debug.
impl std::fmt::Debug for DeltaTAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTAuthSource").finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for DeltaTAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        // Key the lookup exactly as resolve_engine keys engines: missing database means tenant
        // "default", and the name is sanitized so an alias cannot dodge its tenant's credential.
        let tenant_password = crate::tenant::TenantManager::sanitize(login.database().unwrap_or("default"))
            .ok()
            .and_then(|key| self.tenant_passwords.get(&key));
        let password = tenant_password.unwrap_or(&self.password);
        Ok(Password::new(None, password.as_bytes().to_vec()))
    }
}

/// Equality whose timing depends only on the longer input's length, never on where the first
/// mismatching byte sits: the shared password is the entire security boundary, so pgwire's
/// short-circuiting `==` would leak prefix-match timing to the network.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let byte_diff = (0..a.len().max(b.len())).fold(0usize, |acc, i| {
        acc | (usize::from(*a.get(i).unwrap_or(&0)) ^ usize::from(*b.get(i).unwrap_or(&0)))
    });
    (a.len() ^ b.len()) | byte_diff == 0
}

/// The startup handler deltat installs: pgwire's `CleartextPasswordAuthStartupHandler` flow with
/// the password comparison replaced by [`constant_time_eq`].
pub struct DeltaTStartupHandler {
    auth_source: Arc<DeltaTAuthSource>,
    parameter_provider: DefaultServerParameterProvider,
}

impl DeltaTStartupHandler {
    pub fn new(auth_source: Arc<DeltaTAuthSource>) -> Self {
        Self {
            auth_source,
            parameter_provider: DefaultServerParameterProvider::default(),
        }
    }
}

#[async_trait]
impl StartupHandler for DeltaTStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                auth::protocol_negotiation(client, startup).await?;
                auth::save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(pwd) => {
                let pwd = pwd.into_password()?;
                let login_info = LoginInfo::from_client_info(client);
                let expected = self.auth_source.get_password(&login_info).await?;
                if constant_time_eq(expected.password(), pwd.password.as_bytes()) {
                    auth::finish_authentication(client, &self.parameter_provider).await?;
                } else {
                    return Err(PgWireError::InvalidPassword(
                        login_info.user().map(|x| x.to_owned()).unwrap_or_default(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn auth_returns_configured_password() {
        let source = DeltaTAuthSource::new("my_secret".into());
        let login = LoginInfo::new(Some("testuser"), None, "127.0.0.1".to_string());
        let password = source.get_password(&login).await.unwrap();
        assert_eq!(password.password(), b"my_secret");
        assert!(password.salt().is_none());
    }

    fn tenant_map(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(t, p)| (t.to_string(), p.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn tenant_password_overrides_global_only_for_its_tenant() {
        let source = DeltaTAuthSource::with_tenant_passwords(
            "global_pw".into(),
            tenant_map(&[("acme", "acme_pw")]),
        );
        let acme = LoginInfo::new(Some("u"), Some("acme"), "127.0.0.1".to_string());
        assert_eq!(source.get_password(&acme).await.unwrap().password(), b"acme_pw");
        let globex = LoginInfo::new(Some("u"), Some("globex"), "127.0.0.1".to_string());
        assert_eq!(source.get_password(&globex).await.unwrap().password(), b"global_pw");
    }

    #[tokio::test]
    async fn tenant_password_lookup_uses_sanitized_name() {
        // "acme!" resolves to tenant "acme"'s engine, so it must resolve to acme's password too:
        // a raw-name lookup would fall back to the global password and bypass the credential.
        let source = DeltaTAuthSource::with_tenant_passwords(
            "global_pw".into(),
            tenant_map(&[("acme", "acme_pw")]),
        );
        let aliased = LoginInfo::new(Some("u"), Some("acme!"), "127.0.0.1".to_string());
        assert_eq!(source.get_password(&aliased).await.unwrap().password(), b"acme_pw");
    }

    #[tokio::test]
    async fn missing_database_authenticates_against_default_tenant() {
        // resolve_engine maps a missing database to tenant "default"; auth must mirror that.
        let source = DeltaTAuthSource::with_tenant_passwords(
            "global_pw".into(),
            tenant_map(&[("default", "default_pw")]),
        );
        let no_db = LoginInfo::new(Some("u"), None, "127.0.0.1".to_string());
        assert_eq!(source.get_password(&no_db).await.unwrap().password(), b"default_pw");
    }

    #[test]
    fn parse_tenant_passwords_splits_pairs_on_first_colon() {
        let map = parse_tenant_passwords("acme:s1,globex:with:colon").unwrap();
        assert_eq!(map.get("acme"), Some(&"s1".to_string()));
        assert_eq!(map.get("globex"), Some(&"with:colon".to_string()));
    }

    #[test]
    fn parse_tenant_passwords_stores_sanitized_keys() {
        let map = parse_tenant_passwords("ac.me:pw").unwrap();
        assert_eq!(map.get("acme"), Some(&"pw".to_string()));
    }

    #[test]
    fn parse_tenant_passwords_rejects_malformed_input() {
        assert!(parse_tenant_passwords("acme").is_err()); // no colon
        assert!(parse_tenant_passwords("acme:").is_err()); // empty password
        assert!(parse_tenant_passwords(":pw").is_err()); // empty tenant
        assert!(parse_tenant_passwords("acme:x,acme!:y").is_err()); // duplicate after sanitize
    }

    #[test]
    fn constant_time_eq_matches_equality_semantics() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(!constant_time_eq(b"secret", b"secrets"));
        assert!(!constant_time_eq(b"", b"x"));
    }

    #[tokio::test]
    async fn auth_ignores_username() {
        let source = DeltaTAuthSource::new("pass123".into());
        let login1 = LoginInfo::new(Some("alice"), None, "127.0.0.1".to_string());
        let login2 = LoginInfo::new(Some("bob"), None, "127.0.0.1".to_string());
        let p1 = source.get_password(&login1).await.unwrap();
        let p2 = source.get_password(&login2).await.unwrap();
        assert_eq!(p1.password(), p2.password());
    }
}
