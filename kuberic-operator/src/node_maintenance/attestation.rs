use std::collections::BTreeSet;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct Epoch {
    pub data_loss_number: i64,
    pub configuration_number: i64,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CommittedMember {
    pub id: i64,
    pub pod_uid: String,
    pub is_primary: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct CommittedTopology {
    pub epoch: Epoch,
    pub write_quorum: u32,
    pub members: Vec<CommittedMember>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LiveMember {
    pub id: i64,
    pub pod_uid: String,
    pub is_primary: bool,
    pub healthy: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct LiveObservation {
    pub epoch: Epoch,
    pub settled: bool,
    pub primary_pod_uid: Option<String>,
    pub members: Vec<LiveMember>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Attestation {
    Verified,
    Incomplete,
    QuorumLost,
    PrimaryOnNode,
}

pub fn attest(
    committed: Option<&CommittedTopology>,
    live: Option<&LiveObservation>,
    on_node: &BTreeSet<String>,
) -> Attestation {
    let (Some(committed), Some(live)) = (committed, live) else {
        return Attestation::Incomplete;
    };

    if !live.settled || live.epoch != committed.epoch {
        return Attestation::Incomplete;
    }

    let mut attested_survivors = 0usize;
    let mut attested_primary = false;
    for member in &committed.members {
        if on_node.contains(&member.pod_uid) {
            continue;
        }
        let Some(observed) = live
            .members
            .iter()
            .find(|observed| observed.id == member.id)
        else {
            return Attestation::Incomplete;
        };
        if observed.pod_uid != member.pod_uid || observed.is_primary != member.is_primary {
            return Attestation::Incomplete;
        }
        if !observed.healthy {
            continue;
        }
        attested_survivors += 1;
        if member.is_primary && live.primary_pod_uid.as_deref() == Some(member.pod_uid.as_str()) {
            attested_primary = true;
        }
    }

    if attested_survivors < committed.write_quorum as usize {
        return Attestation::QuorumLost;
    }
    if !attested_primary {
        return Attestation::PrimaryOnNode;
    }
    Attestation::Verified
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(configuration_number: i64) -> Epoch {
        Epoch {
            data_loss_number: 0,
            configuration_number,
        }
    }

    fn committed(members: &[(i64, &str, bool)], write_quorum: u32) -> CommittedTopology {
        CommittedTopology {
            epoch: epoch(1),
            write_quorum,
            members: members
                .iter()
                .map(|(id, pod, primary)| CommittedMember {
                    id: *id,
                    pod_uid: (*pod).to_string(),
                    is_primary: *primary,
                })
                .collect(),
        }
    }

    fn live(members: &[(i64, &str, bool, bool)], primary: Option<&str>) -> LiveObservation {
        LiveObservation {
            epoch: epoch(1),
            settled: true,
            primary_pod_uid: primary.map(str::to_string),
            members: members
                .iter()
                .map(|(id, pod, is_primary, healthy)| LiveMember {
                    id: *id,
                    pod_uid: (*pod).to_string(),
                    is_primary: *is_primary,
                    healthy: *healthy,
                })
                .collect(),
        }
    }

    fn on_node(uids: &[&str]) -> BTreeSet<String> {
        uids.iter().map(|uid| (*uid).to_string()).collect()
    }

    #[test]
    fn a_healthy_majority_off_the_node_is_verified() {
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&live(
                &[
                    (1, "a", true, true),
                    (2, "b", false, true),
                    (3, "c", false, true),
                ],
                Some("a"),
            )),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::Verified);
    }

    #[test]
    fn an_unhealthy_survivor_does_not_count_towards_quorum() {
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&live(
                &[
                    (1, "a", true, true),
                    (2, "b", false, false),
                    (3, "c", false, true),
                ],
                Some("a"),
            )),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::QuorumLost);
    }

    #[test]
    fn a_replaced_incarnation_does_not_inherit_evidence() {
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&live(
                &[
                    (1, "a", true, true),
                    (2, "b-restarted", false, true),
                    (3, "c", false, true),
                ],
                Some("a"),
            )),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::Incomplete);
    }

    #[test]
    fn a_missing_observation_is_never_counted_as_quorum() {
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&live(
                &[(1, "a", true, true), (3, "c", false, true)],
                Some("a"),
            )),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::Incomplete);
    }

    #[test]
    fn a_contradicted_role_is_not_trusted() {
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&live(
                &[
                    (1, "a", false, true),
                    (2, "b", true, true),
                    (3, "c", false, true),
                ],
                Some("b"),
            )),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::Incomplete);
    }

    #[test]
    fn a_stale_configuration_is_not_trusted() {
        let mut observation = live(
            &[
                (1, "a", true, true),
                (2, "b", false, true),
                (3, "c", false, true),
            ],
            Some("a"),
        );
        observation.epoch = epoch(2);
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&observation),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::Incomplete);
    }

    #[test]
    fn a_reconfiguration_in_flight_is_not_trusted() {
        let mut observation = live(
            &[
                (1, "a", true, true),
                (2, "b", false, true),
                (3, "c", false, true),
            ],
            Some("a"),
        );
        observation.settled = false;
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&observation),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::Incomplete);
    }

    #[test]
    fn a_primary_still_on_the_node_is_reported() {
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&live(
                &[
                    (1, "a", true, true),
                    (2, "b", false, true),
                    (3, "c", false, true),
                ],
                Some("a"),
            )),
            &on_node(&["a"]),
        );
        assert_eq!(attestation, Attestation::PrimaryOnNode);
    }

    #[test]
    fn a_committed_primary_that_is_not_the_live_primary_is_not_readiness() {
        let attestation = attest(
            Some(&committed(
                &[(1, "a", true), (2, "b", false), (3, "c", false)],
                2,
            )),
            Some(&live(
                &[
                    (1, "a", true, true),
                    (2, "b", false, true),
                    (3, "c", false, true),
                ],
                None,
            )),
            &on_node(&["c"]),
        );
        assert_eq!(attestation, Attestation::PrimaryOnNode);
    }

    #[test]
    fn absent_evidence_is_incomplete() {
        assert_eq!(attest(None, None, &on_node(&[])), Attestation::Incomplete);
        assert_eq!(
            attest(Some(&committed(&[(1, "a", true)], 1)), None, &on_node(&[])),
            Attestation::Incomplete
        );
    }
}
