// Shared by library and executable fixtures without extending the testing API.
// Leave room for PGDATA/pg_stat_tmp/.s.PGSQL.65535 in hosted checkout paths.
pub(crate) const SINGLE_REPLICA_DIRECTORY: &str = "r";
