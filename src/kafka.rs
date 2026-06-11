#![allow(missing_docs)]
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use aws_types::region::Region;
use rdkafka::{
    ClientConfig, ClientContext, Statistics,
    client::OAuthToken,
    consumer::ConsumerContext,
    producer::{DeliveryResult, ProducerContext},
};
use snafu::Snafu;
use tokio::runtime::Handle;
use tracing::Span;
use vector_lib::{configurable::configurable_component, sensitive_string::SensitiveString};

use crate::{
    internal_events::KafkaStatisticsReceived,
    tls::{PEM_START_MARKER, TlsEnableableConfig},
};

#[derive(Debug, Snafu)]
enum KafkaError {
    #[snafu(display("invalid path: {:?}", path))]
    InvalidPath { path: PathBuf },
    #[snafu(display(
        "`msk_iam` cannot be combined with `sasl`; AWS MSK IAM authentication configures SASL automatically"
    ))]
    MskIamSaslConflict,
    #[snafu(display("`msk_iam` requires TLS; `tls.enabled` must not be set to false"))]
    MskIamTlsRequired,
}

/// Supported compression types for Kafka.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default)]
#[serde(rename_all = "lowercase")]
pub enum KafkaCompression {
    /// No compression.
    #[default]
    None,

    /// Gzip.
    Gzip,

    /// Snappy.
    Snappy,

    /// LZ4.
    Lz4,

    /// Zstandard.
    Zstd,
}

/// Kafka authentication configuration.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct KafkaAuthConfig {
    #[configurable(derived)]
    pub(crate) sasl: Option<KafkaSaslConfig>,

    #[configurable(derived)]
    #[configurable(metadata(docs::advanced))]
    pub(crate) tls: Option<TlsEnableableConfig>,

    #[configurable(derived)]
    pub(crate) msk_iam: Option<KafkaMskIamConfig>,
}

/// Configuration for SASL authentication when interacting with Kafka.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct KafkaSaslConfig {
    /// Enables SASL authentication.
    ///
    /// Only `PLAIN`- and `SCRAM`-based mechanisms are supported when configuring SASL authentication using `sasl.*`. For
    /// other mechanisms, `librdkafka_options.*` must be used directly to configure other `librdkafka`-specific values.
    /// If using `sasl.kerberos.*` as an example, where `*` is `service.name`, `principal`, `kinit.md`, etc., then
    /// `librdkafka_options.*` as a result becomes `librdkafka_options.sasl.kerberos.service.name`,
    /// `librdkafka_options.sasl.kerberos.principal`, etc.
    ///
    /// See the [librdkafka documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) for details.
    ///
    /// SASL authentication is not supported on Windows.
    pub(crate) enabled: Option<bool>,

    /// The SASL username.
    #[configurable(metadata(docs::examples = "username"))]
    pub(crate) username: Option<String>,

    /// The SASL password.
    #[configurable(metadata(docs::examples = "password"))]
    pub(crate) password: Option<SensitiveString>,

    /// The SASL mechanism to use.
    #[configurable(metadata(docs::examples = "SCRAM-SHA-256"))]
    #[configurable(metadata(docs::examples = "SCRAM-SHA-512"))]
    pub(crate) mechanism: Option<String>,
}

/// Configuration for AWS MSK IAM authentication.
///
/// When set, Vector authenticates to the cluster using SASL `OAUTHBEARER` tokens signed with
/// the AWS credentials from the default credentials provider chain (environment variables,
/// shared credentials file, IMDS, IRSA, and so on).
///
/// AWS MSK IAM authentication requires TLS, so `tls` must not be disabled. It cannot be
/// combined with `sasl`.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct KafkaMskIamConfig {
    /// The AWS region of the MSK cluster.
    #[configurable(metadata(docs::examples = "us-west-2"))]
    pub(crate) region: String,
}

impl KafkaAuthConfig {
    /// Builds the OAuth token provider used by client contexts to generate AWS MSK IAM
    /// authentication tokens, if MSK IAM authentication is configured.
    ///
    /// Must be called from within a Tokio runtime, whose handle is captured for use by the
    /// token generation callback (which librdkafka invokes from its own threads).
    pub(crate) fn msk_iam_token_provider(&self) -> Option<MskIamTokenProvider> {
        self.msk_iam.as_ref().map(|msk_iam| MskIamTokenProvider {
            region: Region::new(msk_iam.region.clone()),
            handle: Handle::current(),
        })
    }

    pub(crate) fn apply(&self, client: &mut ClientConfig) -> crate::Result<()> {
        let sasl_enabled = self.sasl.as_ref().and_then(|s| s.enabled).unwrap_or(false);
        let msk_iam_enabled = self.msk_iam.is_some();
        // MSK IAM requires TLS, so it is implied unless explicitly disabled (an error below).
        let tls_enabled = self
            .tls
            .as_ref()
            .and_then(|s| s.enabled)
            .unwrap_or(msk_iam_enabled);

        if msk_iam_enabled {
            if sasl_enabled {
                return Err(KafkaError::MskIamSaslConflict.into());
            }
            if !tls_enabled {
                return Err(KafkaError::MskIamTlsRequired.into());
            }
        }

        let protocol = match (sasl_enabled || msk_iam_enabled, tls_enabled) {
            (false, false) => "plaintext",
            (false, true) => "ssl",
            (true, false) => "sasl_plaintext",
            (true, true) => "sasl_ssl",
        };
        client.set("security.protocol", protocol);

        if msk_iam_enabled {
            client.set("sasl.mechanism", "OAUTHBEARER");
        }

        if sasl_enabled {
            let sasl = self.sasl.as_ref().unwrap();
            if let Some(username) = &sasl.username {
                client.set("sasl.username", username.as_str());
            }
            if let Some(password) = &sasl.password {
                client.set("sasl.password", password.inner());
            }
            if let Some(mechanism) = &sasl.mechanism {
                client.set("sasl.mechanism", mechanism);
            }
        }

        if tls_enabled && let Some(tls) = self.tls.as_ref() {
            if let Some(verify_certificate) = &tls.options.verify_certificate {
                client.set(
                    "enable.ssl.certificate.verification",
                    verify_certificate.to_string(),
                );
            }

            if let Some(verify_hostname) = &tls.options.verify_hostname {
                client.set(
                    "ssl.endpoint.identification.algorithm",
                    if *verify_hostname { "https" } else { "none" },
                );
            }

            if let Some(path) = &tls.options.ca_file {
                let text = pathbuf_to_string(path)?;
                if text.contains(PEM_START_MARKER) {
                    client.set("ssl.ca.pem", text);
                } else {
                    client.set("ssl.ca.location", text);
                }
            }

            if let Some(path) = &tls.options.crt_file {
                let text = pathbuf_to_string(path)?;
                if text.contains(PEM_START_MARKER) {
                    client.set("ssl.certificate.pem", text);
                } else {
                    client.set("ssl.certificate.location", text);
                }
            }

            if let Some(path) = &tls.options.key_file {
                let text = pathbuf_to_string(path)?;
                if text.contains(PEM_START_MARKER) {
                    client.set("ssl.key.pem", text);
                } else {
                    client.set("ssl.key.location", text);
                }
            }

            if let Some(pass) = &tls.options.key_pass {
                client.set("ssl.key.password", pass);
            }
        }

        Ok(())
    }
}

fn pathbuf_to_string(path: &Path) -> crate::Result<&str> {
    path.to_str()
        .ok_or_else(|| KafkaError::InvalidPath { path: path.into() }.into())
}

/// Generates SASL `OAUTHBEARER` tokens for AWS MSK IAM authentication.
#[derive(Clone)]
pub(crate) struct MskIamTokenProvider {
    region: Region,
    handle: Handle,
}

impl MskIamTokenProvider {
    /// Generates a fresh MSK IAM authentication token.
    ///
    /// librdkafka invokes the token refresh callback either from one of its own threads or from
    /// the thread polling the client queue, which may be a Tokio worker thread. Blocking on the
    /// async token generation directly could panic or stall the runtime, so it is run on a
    /// short-lived thread instead. Tokens are valid for 15 minutes and librdkafka refreshes them
    /// at 80% of their lifetime, so this is infrequent.
    fn token(&self) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        const TOKEN_GENERATION_TIMEOUT: Duration = Duration::from_secs(10);

        let region = self.region.clone();
        let handle = self.handle.clone();
        let (token, expiration_time_ms) = std::thread::spawn(move || {
            handle.block_on(async {
                tokio::time::timeout(
                    TOKEN_GENERATION_TIMEOUT,
                    aws_msk_iam_sasl_signer::generate_auth_token(region),
                )
                .await
            })
        })
        .join()
        .map_err(|_| "MSK IAM token generation thread panicked")???;

        Ok(OAuthToken {
            token,
            principal_name: String::new(),
            lifetime_ms: expiration_time_ms,
        })
    }
}

pub(crate) struct KafkaStatisticsContext {
    pub(crate) expose_lag_metrics: bool,
    pub span: Span,
    pub(crate) msk_iam_token_provider: Option<MskIamTokenProvider>,
}

impl ClientContext for KafkaStatisticsContext {
    // Enables handling of the token refresh event, which librdkafka only emits when the
    // `OAUTHBEARER` SASL mechanism is configured.
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn stats(&self, statistics: Statistics) {
        // This callback get executed on a separate thread within the rdkafka library, so we need
        // to propagate the span here to attach the component tags to the emitted events.
        let _entered = self.span.enter();
        emit!(KafkaStatisticsReceived {
            statistics: &statistics,
            expose_lag_metrics: self.expose_lag_metrics,
        });
    }

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        match &self.msk_iam_token_provider {
            Some(provider) => provider.token(),
            None => Err("OAUTHBEARER authentication is only supported via `msk_iam`".into()),
        }
    }
}

impl ConsumerContext for KafkaStatisticsContext {}

// Required to use the context with a `BaseProducer` (the sink healthcheck); delivery reports
// are not consumed there.
impl ProducerContext for KafkaStatisticsContext {
    type DeliveryOpaque = ();

    fn delivery(&self, _report: &DeliveryResult<'_>, _opaque: Self::DeliveryOpaque) {}
}

#[cfg(test)]
mod test {
    use super::*;

    fn msk_iam_config() -> KafkaMskIamConfig {
        KafkaMskIamConfig {
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn msk_iam_configures_sasl_ssl_oauthbearer() {
        let auth = KafkaAuthConfig {
            sasl: None,
            tls: None,
            msk_iam: Some(msk_iam_config()),
        };
        let mut client = ClientConfig::new();
        auth.apply(&mut client).unwrap();
        assert_eq!(client.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(client.get("sasl.mechanism"), Some("OAUTHBEARER"));
    }

    #[test]
    fn msk_iam_implies_tls_with_tls_options() {
        let auth = KafkaAuthConfig {
            sasl: None,
            tls: Some(TlsEnableableConfig {
                enabled: None,
                ..Default::default()
            }),
            msk_iam: Some(msk_iam_config()),
        };
        let mut client = ClientConfig::new();
        auth.apply(&mut client).unwrap();
        assert_eq!(client.get("security.protocol"), Some("sasl_ssl"));
    }

    #[test]
    fn msk_iam_conflicts_with_sasl() {
        let auth = KafkaAuthConfig {
            sasl: Some(KafkaSaslConfig {
                enabled: Some(true),
                ..Default::default()
            }),
            tls: None,
            msk_iam: Some(msk_iam_config()),
        };
        let error = auth.apply(&mut ClientConfig::new()).unwrap_err();
        assert!(error.to_string().contains("cannot be combined"));
    }

    #[test]
    fn msk_iam_rejects_disabled_tls() {
        let auth = KafkaAuthConfig {
            sasl: None,
            tls: Some(TlsEnableableConfig {
                enabled: Some(false),
                ..Default::default()
            }),
            msk_iam: Some(msk_iam_config()),
        };
        let error = auth.apply(&mut ClientConfig::new()).unwrap_err();
        assert!(error.to_string().contains("requires TLS"));
    }
}
