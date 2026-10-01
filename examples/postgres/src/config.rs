use std::path::Path;

use crate::instance::PgError;

const HBA_BEGIN: &str = "# --- kuberic managed access begin ---";
const HBA_END: &str = "# --- kuberic managed access end ---";

#[cfg(all(test, feature = "testing"))]
tokio::task_local! {
    pub(crate) static BEFORE_ACCESS_RULES_WRITE: Box<dyn Fn() + Send + Sync>;
}

/// All kuberic-required PostgreSQL configuration in one place.
/// Handles initial setup (after initdb) and patching (after pg_basebackup).
pub struct PgConfig {
    pub port: u16,
    pub socket_dir: String,
}

impl PgConfig {
    pub(crate) async fn disconnect_receiver(&self, data_dir: &Path) -> Result<(), PgError> {
        let path = data_dir.join("postgresql.auto.conf");
        let content = tokio::fs::read_to_string(&path).await.map_err(|error| {
            PgError::Configuration(format!("read receiver configuration: {error}"))
        })?;
        let mut lines = content
            .lines()
            .filter(|line| {
                !line.split_once('=').is_some_and(|(key, _)| {
                    matches!(key.trim(), "primary_conninfo" | "primary_slot_name")
                })
            })
            .collect::<Vec<_>>();
        lines.push("primary_conninfo = ''");
        lines.push("primary_slot_name = ''");
        Self::replace_config(&path, &(lines.join("\n") + "\n")).await
    }

    pub fn new(port: u16, data_dir: &Path) -> Self {
        Self {
            port,
            socket_dir: data_dir.join("pg_stat_tmp").to_string_lossy().to_string(),
        }
    }

    pub async fn configure_standby(
        &self,
        data_dir: &Path,
        host: &str,
        port: u16,
        application_name: &str,
        slot: &str,
        lineage: &crate::build::PgLineage,
    ) -> Result<(), PgError> {
        lineage.validate().map_err(PgError::Configuration)?;
        let timeline = lineage.timeline;
        if host.parse::<std::net::IpAddr>().is_err()
            || port == 0
            || timeline == 0
            || application_name.is_empty()
            || application_name.len() > 63
            || !application_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            || !slot.starts_with("kuberic_")
            || slot.len() != 40
            || !slot[8..].bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(PgError::Configuration(
                "invalid exact standby source".into(),
            ));
        }
        self.write_initial(data_dir).await?;
        let path = data_dir.join("postgresql.auto.conf");
        let content = tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| PgError::Configuration(format!("read standby configuration: {e}")))?;
        let mut lines = content
            .lines()
            .filter(|line| {
                !line.split_once('=').is_some_and(|(key, _)| {
                    matches!(
                        key.trim(),
                        "primary_conninfo" | "primary_slot_name" | "synchronous_standby_names"
                    ) || key.trim().starts_with("recovery_target")
                })
            })
            .map(str::to_owned)
            .collect::<Vec<_>>();
        lines.push(format!(
            "primary_conninfo = 'host={host} port={port} application_name={application_name}'"
        ));
        lines.push(format!("recovery_target_timeline = '{timeline}'"));
        lines.push(format!("primary_slot_name = '{slot}'"));
        lines.push("synchronous_standby_names = ''".into());
        Self::replace_config(&path, &(lines.join("\n") + "\n")).await?;
        if timeline > 1 {
            Self::replace_config(
                &data_dir
                    .join("pg_wal")
                    .join(format!("{timeline:08X}.history")),
                &lineage.history_text,
            )
            .await?;
        }
        Self::replace_config(&data_dir.join("standby.signal"), "").await
    }

    /// Idempotently repair managed settings and close external access before startup.
    pub async fn write_initial(&self, data_dir: &Path) -> Result<(), PgError> {
        // PostgreSQL backup/rewind exclude this transient directory. Live socket
        // files in PGDATA itself otherwise make a real divergent rewind fail.
        tokio::fs::create_dir_all(&self.socket_dir)
            .await
            .map_err(|error| PgError::Configuration(format!("create socket directory: {error}")))?;
        self.write_postgresql_conf(data_dir).await?;
        self.write_pg_hba_conf(data_dir).await?;
        Ok(())
    }

    pub async fn patch_after_clone_exact(
        &self,
        data_dir: &Path,
        application_name: &str,
    ) -> Result<(), PgError> {
        if application_name.is_empty() || application_name.len() > 63 {
            return Err(PgError::Configuration(
                "replication application name must contain 1-63 bytes".into(),
            ));
        }
        self.patch_postgresql_conf(data_dir).await?;
        self.patch_primary_conninfo(data_dir, application_name)
            .await?;
        Ok(())
    }

    async fn write_postgresql_conf(&self, data_dir: &Path) -> Result<(), PgError> {
        let conf_path = data_dir.join("postgresql.conf");
        let existing = tokio::fs::read_to_string(&conf_path)
            .await
            .map_err(|e| PgError::Process(format!("read postgresql.conf: {e}")))?;
        let managed = [
            "port",
            "unix_socket_directories",
            "wal_log_hints",
            "hot_standby",
            "synchronous_commit",
            "logging_collector",
            "listen_addresses",
            "wal_level",
            "max_wal_senders",
            "max_replication_slots",
        ];
        let existing = existing
            .lines()
            .filter(|line| {
                line.trim() != "# --- kuberic required settings ---"
                    && !line
                        .split_once('=')
                        .is_some_and(|(key, _)| managed.contains(&key.trim()))
            })
            .collect::<Vec<_>>()
            .join("\n");

        let kuberic_conf = format!(
            r#"
# --- kuberic required settings ---
port = {port}
unix_socket_directories = '{socket_dir}'
wal_log_hints = on
hot_standby = on
synchronous_commit = remote_apply
logging_collector = off
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 32
"#,
            port = self.port,
            socket_dir = encode_postgres_setting(&self.socket_dir),
        );

        Self::replace_config(
            &conf_path,
            &format!("{}\n{kuberic_conf}", existing.trim_end()),
        )
        .await
    }

    /// Write pg_hba.conf entries for replication + trust auth.
    async fn write_pg_hba_conf(&self, data_dir: &Path) -> Result<(), PgError> {
        let hba_path = data_dir.join("pg_hba.conf");
        let content = tokio::fs::read_to_string(&hba_path)
            .await
            .map_err(|e| PgError::Process(format!("read pg_hba.conf: {e}")))?;
        let content = replace_managed_hba(&content, false, "kuberic_app", "kuberic");
        Self::replace_config(&hba_path, &content).await
    }

    async fn replace_config(path: &Path, content: &str) -> Result<(), PgError> {
        Self::replace_config_file(path, content)
            .map_err(|e| PgError::Process(format!("repair {}: {e}", path.display())))
    }

    fn replace_config_file(path: &Path, content: &str) -> std::io::Result<()> {
        use std::io::Write;

        // Keep the bounded file replacement within one poll. A cancelled grant
        // must not leave a background rename that can overwrite a later closed
        // HBA generation after its access lock has been released.
        let pending = path.with_extension("conf.pending");
        let mut file = std::fs::File::create(&pending)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&pending, path)?;
        std::fs::File::open(path.parent().expect("configuration has a parent"))?.sync_all()
    }

    /// Fix port and socket_dir in postgresql.conf after pg_basebackup.
    /// pg_basebackup copies the primary's config which has wrong values.
    async fn patch_postgresql_conf(&self, data_dir: &Path) -> Result<(), PgError> {
        let conf_path = data_dir.join("postgresql.conf");
        let content = tokio::fs::read_to_string(&conf_path)
            .await
            .map_err(|e| PgError::Process(format!("read postgresql.conf: {e}")))?;

        let mut new_lines: Vec<String> = content
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.starts_with("port ")
                    && !trimmed.starts_with("port=")
                    && !trimmed.starts_with("unix_socket_directories")
            })
            .map(String::from)
            .collect();

        new_lines.push(format!("port = {}", self.port));
        new_lines.push(format!(
            "unix_socket_directories = '{}'",
            encode_postgres_setting(&self.socket_dir)
        ));

        Self::replace_config(&conf_path, &(new_lines.join("\n") + "\n")).await?;

        tracing::debug!(port = self.port, "postgresql.conf patched after clone");
        Ok(())
    }

    /// Set application_name in primary_conninfo (postgresql.auto.conf).
    /// pg_basebackup -R writes primary_conninfo with default application_name.
    /// Exact identity/session names are used for synchronous standby matching.
    async fn patch_primary_conninfo(
        &self,
        data_dir: &Path,
        application_name: &str,
    ) -> Result<(), PgError> {
        let auto_conf = data_dir.join("postgresql.auto.conf");
        let content = tokio::fs::read_to_string(&auto_conf)
            .await
            .map_err(|e| PgError::Process(format!("read auto.conf: {e}")))?;

        let new_lines: Vec<String> = content
            .lines()
            .map(|line| {
                if line.contains("primary_conninfo") {
                    return replace_primary_conninfo_application_name(line, application_name);
                }
                Ok(line.to_string())
            })
            .collect::<Result<_, _>>()?;

        Self::replace_config(&auto_conf, &(new_lines.join("\n") + "\n")).await?;

        tracing::debug!(application_name, "application_name set in primary_conninfo");
        Ok(())
    }

    pub async fn write_access_rules(
        data_dir: &Path,
        granted: bool,
        application_role: &str,
        application_database: &str,
    ) -> Result<(), PgError> {
        let hba_path = data_dir.join("pg_hba.conf");
        let content = tokio::fs::read_to_string(&hba_path)
            .await
            .map_err(|error| PgError::Process(format!("read pg_hba.conf: {error}")))?;
        let content =
            replace_managed_hba(&content, granted, application_role, application_database);
        #[cfg(all(test, feature = "testing"))]
        let _ = BEFORE_ACCESS_RULES_WRITE.try_with(|before_write| before_write());
        Self::replace_config_file(&hba_path, &content)
            .map_err(|error| PgError::Process(format!("write pg_hba.conf: {error}")))
    }
}

fn replace_primary_conninfo_application_name(
    line: &str,
    application_name: &str,
) -> Result<String, PgError> {
    let start = line
        .find('\'')
        .ok_or_else(|| PgError::Configuration("primary_conninfo lacks opening quote".into()))?;
    let end = line
        .rfind('\'')
        .filter(|end| *end > start)
        .ok_or_else(|| PgError::Configuration("primary_conninfo lacks closing quote".into()))?;
    let sql_value = decode_postgres_setting(&line[start + 1..end])?;
    let mut fields = parse_conninfo(&sql_value)?;
    fields.retain(|(key, _)| key != "application_name");
    fields.push(("application_name".into(), application_name.into()));
    let serialized = fields
        .into_iter()
        .map(|(key, value)| format!("{key}='{}'", escape_conninfo_value(&value)))
        .collect::<Vec<_>>()
        .join(" ");
    Ok(format!(
        "{}'{}'{}",
        &line[..start],
        encode_postgres_setting(&serialized),
        &line[end + 1..]
    ))
}

fn decode_postgres_setting(value: &str) -> Result<String, PgError> {
    let characters = value.chars().collect::<Vec<_>>();
    let mut decoded = String::new();
    let mut index = 0;
    while index < characters.len() {
        match characters[index] {
            '\'' if index + 1 < characters.len() && characters[index + 1] == '\'' => {
                decoded.push('\'');
                index += 2;
            }
            '\\' if index + 1 < characters.len() => {
                decoded.push(characters[index + 1]);
                index += 2;
            }
            '\\' => {
                return Err(PgError::Configuration(
                    "primary_conninfo contains a trailing escape".into(),
                ));
            }
            character => {
                decoded.push(character);
                index += 1;
            }
        }
    }
    Ok(decoded)
}

fn encode_postgres_setting(value: &str) -> String {
    let mut encoded = String::new();
    for character in value.chars() {
        match character {
            '\\' => encoded.push_str("\\\\"),
            '\'' => encoded.push_str("''"),
            _ => encoded.push(character),
        }
    }
    encoded
}

fn parse_conninfo(value: &str) -> Result<Vec<(String, String)>, PgError> {
    let characters = value.chars().collect::<Vec<_>>();
    let mut index = 0;
    let mut fields = Vec::new();
    while index < characters.len() {
        while index < characters.len() && characters[index].is_whitespace() {
            index += 1;
        }
        if index == characters.len() {
            break;
        }
        let key_start = index;
        while index < characters.len() && characters[index] != '=' {
            index += 1;
        }
        if index == characters.len() {
            return Err(PgError::Configuration(
                "primary_conninfo contains a field without '='".into(),
            ));
        }
        let key = characters[key_start..index]
            .iter()
            .collect::<String>()
            .trim()
            .to_string();
        index += 1;
        let mut field = String::new();
        if index < characters.len() && characters[index] == '\'' {
            index += 1;
            let mut closed = false;
            while index < characters.len() {
                match characters[index] {
                    '\\' if index + 1 < characters.len() => {
                        index += 1;
                        field.push(characters[index]);
                    }
                    '\'' => {
                        closed = true;
                        index += 1;
                        break;
                    }
                    character => field.push(character),
                }
                index += 1;
            }
            if !closed {
                return Err(PgError::Configuration(
                    "primary_conninfo contains an unterminated quoted value".into(),
                ));
            }
        } else {
            while index < characters.len() && !characters[index].is_whitespace() {
                if characters[index] == '\\' && index + 1 < characters.len() {
                    index += 1;
                }
                field.push(characters[index]);
                index += 1;
            }
        }
        if key.is_empty() {
            return Err(PgError::Configuration(
                "primary_conninfo contains an empty key".into(),
            ));
        }
        fields.push((key, field));
    }
    Ok(fields)
}

fn escape_conninfo_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "\\'")
}

fn replace_managed_hba(
    existing: &str,
    granted: bool,
    application_role: &str,
    application_database: &str,
) -> String {
    let mut retained = Vec::new();
    let mut managed = false;
    for line in existing.lines() {
        if line.trim() == HBA_BEGIN {
            managed = true;
            continue;
        }
        if line.trim() == HBA_END {
            managed = false;
            continue;
        }
        if !managed {
            retained.push(line);
        }
    }
    let internal_user = whoami::username().unwrap_or_else(|_| "postgres".into());
    let application = if granted {
        format!(
            "local   {application_database}   {application_role}                 trust\n\
             host    {application_database}   {application_role} 127.0.0.1/32    trust\n\
             host    {application_database}   {application_role} ::1/128         trust\n"
        )
    } else {
        String::new()
    };
    format!(
        "{HBA_BEGIN}\n\
         local   replication     all                                trust\n\
         host    replication     all              127.0.0.1/32      trust\n\
         host    replication     all              ::1/128           trust\n\
         host    replication     all              0.0.0.0/0         trust\n\
         local   all             {internal_user}                    trust\n\
         {application}\
         local   all             all                                reject\n\
         host    postgres        kuberic_rewind   127.0.0.1/32      trust\n\
         host    postgres        kuberic_rewind   ::1/128           trust\n\
         host    all             all              0.0.0.0/0         reject\n\
         host    all             all              ::0/0             reject\n\
         {HBA_END}\n{}\n",
        retained.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn hba_access_rules_preserve_retained_content_and_error_context() {
        let directory = crate::testing::TestDataDir::new("hba-rules");
        let path = directory.path().join("pg_hba.conf");
        let missing = PgConfig::write_access_rules(directory.path(), true, "test_app", "test_db")
            .await
            .unwrap_err();
        assert!(
            matches!(missing, PgError::Process(message) if message.starts_with("read pg_hba.conf: "))
        );

        let retained = "# operator-owned rule\nhost all auditor 192.0.2.0/24 reject\n";
        std::fs::write(&path, retained).unwrap();
        for granted in [true, false, true, false] {
            PgConfig::write_access_rules(directory.path(), granted, "test_app", "test_db")
                .await
                .unwrap();
            let content = std::fs::read_to_string(&path).unwrap();
            assert!(content.ends_with(retained));
            assert_eq!(content.matches(HBA_BEGIN).count(), 1);
            assert_eq!(content.matches(HBA_END).count(), 1);
            assert_eq!(content.contains("test_app"), granted);
            assert_eq!(content.contains("test_db"), granted);
            assert!(!path.with_extension("conf.pending").exists());
        }

        let closed = std::fs::read(&path).unwrap();
        std::fs::create_dir(path.with_extension("conf.pending")).unwrap();
        let failed = PgConfig::write_access_rules(directory.path(), true, "test_app", "test_db")
            .await
            .unwrap_err();
        assert!(
            matches!(failed, PgError::Process(message) if message.starts_with("write pg_hba.conf: "))
        );
        assert_eq!(std::fs::read(&path).unwrap(), closed);
    }

    #[test]
    fn exact_application_name_replaces_quoted_conninfo_value() {
        let line =
            "primary_conninfo = 'host=127.0.0.1 application_name=''old session'' user=postgres'";
        let replaced = replace_primary_conninfo_application_name(line, "exact_new").unwrap();
        let start = replaced.find('\'').unwrap();
        let end = replaced.rfind('\'').unwrap();
        let decoded = decode_postgres_setting(&replaced[start + 1..end]).unwrap();
        let fields = parse_conninfo(&decoded).unwrap();
        assert_eq!(
            fields
                .iter()
                .find(|(key, _)| key == "application_name")
                .map(|(_, value)| value.as_str()),
            Some("exact_new")
        );
        assert_eq!(
            fields
                .iter()
                .find(|(key, _)| key == "user")
                .map(|(_, value)| value.as_str()),
            Some("postgres")
        );

        let escaped = "primary_conninfo = 'host=127.0.0.1 password=''a\\\\''b'' application_name=''old session'''";
        let replaced = replace_primary_conninfo_application_name(escaped, "new_session").unwrap();
        let start = replaced.find('\'').unwrap();
        let end = replaced.rfind('\'').unwrap();
        let decoded = decode_postgres_setting(&replaced[start + 1..end]).unwrap();
        let fields = parse_conninfo(&decoded).unwrap();
        assert_eq!(
            fields
                .iter()
                .find(|(key, _)| key == "password")
                .map(|(_, value)| value.as_str()),
            Some("a'b")
        );

        let unquoted = "primary_conninfo = 'host=127.0.0.1 password=a\\\\''b application_name=old'";
        let replaced = replace_primary_conninfo_application_name(unquoted, "new_session").unwrap();
        let start = replaced.find('\'').unwrap();
        let end = replaced.rfind('\'').unwrap();
        let decoded = decode_postgres_setting(&replaced[start + 1..end]).unwrap();
        let fields = parse_conninfo(&decoded).unwrap();
        assert_eq!(
            fields
                .iter()
                .find(|(key, _)| key == "password")
                .map(|(_, value)| value.as_str()),
            Some("a'b")
        );
    }
}
