//! Runtime settings persisted on the data volume (survives restarts).
//!
//! Everything here is editable from the dashboard's settings section: the
//! domain family the gateway serves, the acceleration host handed to the
//! Docker daemon for pull-as-a-service, and the Cloudflare credentials used
//! to (a) create the matching DNS records and (b) answer DNS-01 challenges
//! when issuing the public certificate.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde_json::{json, Value};

#[derive(Clone, Debug, Default)]
pub struct Settings {
    /// Primary domain (panel/realm), e.g. "master.us.kg".
    pub domain: Option<String>,
    /// Acceleration host for docker/git pulls, e.g. "docker.master.us.kg".
    pub accel_domain: Option<String>,
    /// Port appended to the acceleration host (default 4443).
    pub accel_port: Option<u16>,
    /// LAN IP used when creating DNS A records (e.g. "192.168.1.107").
    pub lan_ip: Option<String>,
    /// Cloudflare API token, DNS-edit scoped. Write-only: never returned.
    pub cf_token: Option<String>,
    pub cf_zone_id: Option<String>,
    pub cf_account_id: Option<String>,
}

/// Field updates applied on top of the stored settings. `None` = unchanged.
#[derive(Clone, Debug, Default)]
pub struct SettingsPatch {
    pub domain: Option<String>,
    pub accel_domain: Option<String>,
    pub accel_port: Option<u16>,
    pub lan_ip: Option<String>,
    pub cf_token: Option<String>,
    pub cf_zone_id: Option<String>,
    pub cf_account_id: Option<String>,
}

impl Settings {
    pub fn load(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(text) => Self::from_json(&serde_json::from_str(&text).unwrap_or(Value::Null)),
            Err(_) => Self::default(),
        }
    }

    pub fn from_json(value: &Value) -> Self {
        let s = |key: &str| {
            value
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
                .filter(|v| !v.is_empty())
        };
        Self {
            domain: s("domain"),
            accel_domain: s("accel_domain"),
            accel_port: value
                .get("accel_port")
                .and_then(|v| v.as_u64())
                .map(|v| v as u16),
            lan_ip: s("lan_ip"),
            cf_token: s("cf_token"),
            cf_zone_id: s("cf_zone_id"),
            cf_account_id: s("cf_account_id"),
        }
    }

    /// Apply a patch and persist. Returns the updated settings.
    pub fn apply(self, patch: SettingsPatch, path: &Path) -> Result<Self> {
        let merged = Settings {
            domain: patch.domain.or(self.domain),
            accel_domain: patch.accel_domain.or(self.accel_domain),
            accel_port: patch.accel_port.or(self.accel_port),
            lan_ip: patch.lan_ip.or(self.lan_ip),
            cf_token: patch.cf_token.or(self.cf_token),
            cf_zone_id: patch.cf_zone_id.or(self.cf_zone_id),
            cf_account_id: patch.cf_account_id.or(self.cf_account_id),
        };
        merged.validate()?;
        merged.save(path)?;
        Ok(merged)
    }

    fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("domain", &self.domain),
            ("accel_domain", &self.accel_domain),
            ("lan_ip", &self.lan_ip),
        ] {
            if let Some(v) = value {
                if v.starts_with('.')
                    || v.starts_with('-')
                    || v.contains("://")
                    || v.contains("..")
                    || !v.chars().all(|c| {
                        c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-')
                    })
                {
                    anyhow::bail!("invalid {label}: {v}");
                }
            }
        }
        if let Some(port) = self.accel_port {
            if port == 0 {
                anyhow::bail!("invalid accel_port");
            }
        }
        Ok(())
    }

    fn to_disk_json(&self) -> Value {
        json!({
            "domain": self.domain,
            "accel_domain": self.accel_domain,
            "accel_port": self.accel_port,
            "lan_ip": self.lan_ip,
            "cf_token": self.cf_token,
            "cf_zone_id": self.cf_zone_id,
            "cf_account_id": self.cf_account_id,
        })
    }

    fn to_json(&self) -> Value {
        // cf_token is deliberately omitted from views: it is write-only.
        json!({
            "domain": self.domain,
            "accel_domain": self.accel_domain,
            "accel_port": self.accel_port,
            "lan_ip": self.lan_ip,
            "cf_zone_id": self.cf_zone_id,
            "cf_account_id": self.cf_account_id,
            "cf_token_set": self.cf_token.is_some(),
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create {}", parent.display()))?;
        }
        std::fs::write(path, self.to_disk_json().to_string())
            .with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }

    /// The host:port the Docker daemon should pull through, from settings.
    pub fn pull_target(&self) -> Option<String> {
        let accel = self.accel_domain.as_deref()?;
        match self.accel_port {
            Some(port) => Some(format!("{accel}:{port}")),
            None => Some(accel.to_owned()),
        }
    }
}

/// Masked view for GET /settings: values the dashboard may show.
pub fn settings_view(settings: &Settings, extra: Value) -> Value {
    let mut base = settings.to_json();
    if let Some(obj) = base.as_object_mut() {
        if let Some(extra) = extra.as_object() {
            for (k, v) in extra {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    base
}

/// Parse a POST /settings body into a patch. Missing keys stay unchanged;
/// an explicit null clears nothing (fields are optional anyway), so only
/// present string fields are applied.
pub fn patch_from_body(body: &Value) -> SettingsPatch {
    let s = |key: &str| {
        body.get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
    };
    SettingsPatch {
        domain: s("domain"),
        accel_domain: s("accel_domain"),
        accel_port: body
            .get("accel_port")
            .and_then(|v| v.as_u64())
            .map(|v| v as u16),
        lan_ip: s("lan_ip"),
        cf_token: s("cf_token"),
        cf_zone_id: s("cf_zone_id"),
        cf_account_id: s("cf_account_id"),
    }
}

pub fn settings_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("settings.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_missing_file_is_default() {
        let s = Settings::load(Path::new("/nonexistent/settings.json"));
        assert_eq!(s.domain, None);
        assert_eq!(s.cf_token, None);
    }

    #[test]
    fn save_and_round_trip_keeps_token_on_disk() {
        // The token must survive to disk (issuance reads it back); masking
        // is applied in settings_view for the API, not in the stored file.
        let tmp = std::env::temp_dir().join("wp-settings-test.json");
        let _ = std::fs::remove_file(&tmp);
        let s = Settings::from_json(&json!({
            "domain": "master.us.kg",
            "cf_token": "secret",
        }));
        s.save(&tmp).unwrap();
        let loaded = Settings::load(&tmp);
        assert_eq!(loaded.domain.as_deref(), Some("master.us.kg"));
        assert_eq!(loaded.cf_token.as_deref(), Some("secret"));
        assert!(
            settings_view(&loaded, Value::Null)
                .to_string()
                .contains("secret")
                == false
        );
        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn view_exposes_only_masked_token_flag() {
        let s = Settings {
            cf_token: Some("real-token".into()),
            domain: Some("master.us.kg".into()),
            ..Default::default()
        };
        let view = settings_view(&s, json!({"cert_status": "idle"}));
        assert_eq!(view["cf_token_set"], json!(true));
        assert!(view.get("cf_token").is_none());
        assert_eq!(view["cert_status"], json!("idle"));
    }

    #[test]
    fn pull_target_combines_domain_and_port() {
        let mut s = Settings::default();
        assert_eq!(s.pull_target(), None);
        s.accel_domain = Some("docker.master.us.kg".into());
        assert_eq!(s.pull_target().as_deref(), Some("docker.master.us.kg"));
        s.accel_port = Some(4443);
        assert_eq!(s.pull_target().as_deref(), Some("docker.master.us.kg:4443"));
    }

    #[test]
    fn patch_only_touches_present_fields() {
        let base = Settings {
            domain: Some("old.example".into()),
            ..Default::default()
        };
        let patch = patch_from_body(&json!({ "domain": "new.example" }));
        let merged = base.apply(patch, Path::new("/nonexistent/x.json")).unwrap();
        assert_eq!(merged.domain.as_deref(), Some("new.example"));
        assert_eq!(merged.cf_token, None);
    }

    #[test]
    fn validate_rejects_bad_values() {
        let s = Settings {
            domain: Some("a<img>b".into()),
            ..Default::default()
        };
        assert!(s.validate().is_err());
        let s = Settings {
            accel_port: Some(0),
            ..Default::default()
        };
        assert!(s.validate().is_err());
    }
}
