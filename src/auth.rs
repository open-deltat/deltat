//! Cleartext-password auth for the pgwire startup handshake: one shared server password
//! (`DELTAT_PASSWORD`) verified against every connection.

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
}

impl DeltaTAuthSource {
    pub fn new(password: String) -> Self {
        Self { password }
    }
}

// Redact the shared password so it can never reach a log line through a derived Debug.
impl std::fmt::Debug for DeltaTAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaTAuthSource").finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for DeltaTAuthSource {
    async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
        Ok(Password::new(None, self.password.as_bytes().to_vec()))
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
