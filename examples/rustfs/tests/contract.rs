use rustfs_native::{
    ConfigError, DRIVES_PER_PEER, ERASURE_SET_DRIVE_COUNT, PEER_COUNT, Peer, RustFsConfig,
    Topology, Transport,
};

fn drives() -> Vec<String> {
    (1..=DRIVES_PER_PEER)
        .map(|index| format!("/data/rustfs{index}"))
        .collect()
}

fn peer(host: &str) -> Peer {
    Peer::new(host, 9000, drives()).unwrap()
}

fn peers() -> Vec<Peer> {
    (0..PEER_COUNT)
        .map(|index| peer(&format!("rustfs-{index}.rustfs")))
        .collect()
}

fn topology() -> Topology {
    Topology::new(Transport::Http, peers()).unwrap()
}

#[test]
fn supported_profile_has_four_peers_and_sixteen_volumes() {
    assert_eq!(PEER_COUNT, 4);
    assert_eq!(DRIVES_PER_PEER, 4);
    assert_eq!(ERASURE_SET_DRIVE_COUNT, 16);
    let topology = topology();
    assert_eq!(topology.peers().len(), 4);
    assert_eq!(topology.volume_arguments().len(), 16);
    assert_eq!(topology.volume_arguments().len(), ERASURE_SET_DRIVE_COUNT);
}

#[test]
fn volume_arguments_are_explicit_and_preserve_peer_and_drive_order() {
    assert_eq!(
        topology().volume_arguments(),
        [
            "http://rustfs-0.rustfs:9000/data/rustfs1",
            "http://rustfs-0.rustfs:9000/data/rustfs2",
            "http://rustfs-0.rustfs:9000/data/rustfs3",
            "http://rustfs-0.rustfs:9000/data/rustfs4",
            "http://rustfs-1.rustfs:9000/data/rustfs1",
            "http://rustfs-1.rustfs:9000/data/rustfs2",
            "http://rustfs-1.rustfs:9000/data/rustfs3",
            "http://rustfs-1.rustfs:9000/data/rustfs4",
            "http://rustfs-2.rustfs:9000/data/rustfs1",
            "http://rustfs-2.rustfs:9000/data/rustfs2",
            "http://rustfs-2.rustfs:9000/data/rustfs3",
            "http://rustfs-2.rustfs:9000/data/rustfs4",
            "http://rustfs-3.rustfs:9000/data/rustfs1",
            "http://rustfs-3.rustfs:9000/data/rustfs2",
            "http://rustfs-3.rustfs:9000/data/rustfs3",
            "http://rustfs-3.rustfs:9000/data/rustfs4",
        ]
    );
}

#[test]
fn rendering_does_not_sort_explicit_configuration() {
    let mut configured = peers();
    configured.swap(0, 3);
    let reordered_drives = vec![
        "/z".to_string(),
        "/a".to_string(),
        "/d10".to_string(),
        "/d2".to_string(),
    ];
    configured[0] = Peer::new("rustfs-3.rustfs", 9000, reordered_drives).unwrap();
    let topology = Topology::new(Transport::Http, configured).unwrap();
    assert_eq!(
        &topology.volume_arguments()[..4],
        [
            "http://rustfs-3.rustfs:9000/z",
            "http://rustfs-3.rustfs:9000/a",
            "http://rustfs-3.rustfs:9000/d10",
            "http://rustfs-3.rustfs:9000/d2",
        ]
    );
}

#[test]
fn all_local_peers_use_the_same_complete_volume_list() {
    let topology = topology();
    for peer in topology.peers() {
        let config = RustFsConfig::new(topology.clone(), peer.host()).unwrap();
        assert_eq!(config.local_peer(), peer);
        assert_eq!(
            config.topology().volume_arguments(),
            topology.volume_arguments()
        );
    }
}

#[test]
fn https_is_applied_to_every_volume() {
    let topology = Topology::new(Transport::Https, peers()).unwrap();
    assert_eq!(topology.transport(), Transport::Https);
    assert!(
        topology
            .volume_arguments()
            .iter()
            .all(|argument| argument.starts_with("https://"))
    );
}

#[test]
fn dns_case_is_canonicalized_for_identity_and_rendering() {
    let lower = peer("rustfs-0.rustfs");
    let upper = peer("RUSTFS-0.RUSTFS");
    assert_eq!(upper, lower);
    assert_eq!(upper.host(), "rustfs-0.rustfs");
    let config = RustFsConfig::new(topology(), "RUSTFS-0.RUSTFS").unwrap();
    assert_eq!(config.local_peer(), &lower);
}

#[test]
fn invalid_hosts_are_rejected_without_echoing_input() {
    for host in [
        "",
        " ",
        " rustfs-0",
        "rustfs-0 ",
        "rustfs-0\n",
        "-rustfs",
        "rustfs-",
        "rust_fs",
        "rustfs..svc",
        ".rustfs",
        "rustfs.",
        "http://rustfs-0",
        "rustfs:9000",
        "user:password@rustfs",
        "rustfs/path",
        "rustfs?query",
        "rustfs#fragment",
        "rustfs{0...3}",
        "r\u{fc}stfs",
        "127.0.0.1",
        "127.1",
        "2130706433",
        "0x7f000001",
        "0X7F000001",
        "0x7f.0x0.0x0.0x1",
        "0177.0.0.1",
        "rustfs.123",
        "rustfs.0xff",
        "0.0.0.0",
        "::1",
        "[::1]",
    ] {
        let error = Peer::new(host, 9000, drives()).unwrap_err();
        assert_eq!(error, ConfigError::InvalidHost, "{host:?}");
        assert_eq!(
            error.to_string(),
            "peer host must be a stable ASCII DNS name, not an IP address"
        );
    }
}

#[test]
fn dns_label_length_boundary_is_enforced() {
    assert!(Peer::new("a".repeat(63), 9000, drives()).is_ok());
    assert_eq!(
        Peer::new("a".repeat(64), 9000, drives()),
        Err(ConfigError::InvalidHost)
    );
}

#[test]
fn dns_name_length_boundary_is_enforced() {
    let prefix = ["a".repeat(63), "b".repeat(63), "c".repeat(63)].join(".");
    let at_limit = format!("{prefix}.{}", "d".repeat(61));
    assert_eq!(at_limit.len(), 253);
    assert!(Peer::new(at_limit.clone(), 9000, drives()).is_ok());
    assert_eq!(
        Peer::new(format!("{at_limit}d"), 9000, drives()),
        Err(ConfigError::InvalidHost)
    );
}

#[test]
fn port_must_be_nonzero() {
    assert_eq!(
        Peer::new("rustfs-0", 0, drives()),
        Err(ConfigError::InvalidPort)
    );
    for port in [1, 9000, u16::MAX] {
        assert_eq!(Peer::new("rustfs-0", port, drives()).unwrap().port(), port);
    }
}

#[test]
fn drive_count_is_fixed() {
    for count in [0, 1, 3, 5, 16] {
        let configured = (0..count).map(|index| format!("/disk{index}")).collect();
        assert_eq!(
            Peer::new("rustfs-0", 9000, configured),
            Err(ConfigError::UnsupportedDriveCount { actual: count })
        );
    }
}

#[test]
fn invalid_drive_paths_are_rejected_on_every_platform() {
    for path in [
        "",
        "/",
        "data",
        "./data",
        "../data",
        "/data/",
        "//data",
        "/data//disk",
        "/data/./disk",
        "/data/../disk",
        "/data/.",
        "/data/..",
        "/data disk",
        "/data\n",
        "/data\t",
        "/data\0",
        "C:\\data",
        "\\data",
        "/data\\disk",
        "/data?query",
        "/data#fragment",
        "/data%2Fdisk",
        "/data{1...4}",
        "/data;command",
        "/data$(command)",
        "/d\u{e1}ta",
    ] {
        let mut configured = drives();
        configured[2] = path.to_string();
        assert_eq!(
            Peer::new("rustfs-0", 9000, configured),
            Err(ConfigError::InvalidDrivePath { index: 2 }),
            "{path:?}"
        );
    }
}

#[test]
fn drive_case_and_supported_components_are_preserved() {
    let configured = vec![
        "/DATA/drive-1".to_string(),
        "/data/drive_2".to_string(),
        "/data/drive.3".to_string(),
        "/data/.drive4".to_string(),
    ];
    let peer = Peer::new("rustfs-0", 9000, configured.clone()).unwrap();
    assert_eq!(peer.drives(), configured);
}

#[test]
fn duplicate_drives_are_rejected() {
    let mut configured = drives();
    configured[3] = configured[0].clone();
    assert_eq!(
        Peer::new("rustfs-0", 9000, configured),
        Err(ConfigError::DuplicateDrive {
            first: 0,
            second: 3
        })
    );
}

#[test]
fn nested_drives_are_rejected_in_either_order() {
    for paths in [["/disk", "/disk/nested"], ["/disk/nested", "/disk"]] {
        let mut configured = drives();
        configured[0] = paths[0].to_string();
        configured[1] = paths[1].to_string();
        assert_eq!(
            Peer::new("rustfs-0", 9000, configured),
            Err(ConfigError::OverlappingDrives {
                first: 0,
                second: 1
            })
        );
    }
}

#[test]
fn a_shared_path_prefix_is_not_a_nested_drive() {
    let configured = ["/disk", "/disk1", "/disk-2", "/disk_3"]
        .map(String::from)
        .to_vec();
    assert!(Peer::new("rustfs-0", 9000, configured).is_ok());
}

#[test]
fn peer_count_is_fixed() {
    for count in [0, 1, 3, 5, 16] {
        let configured = (0..count)
            .map(|index| peer(&format!("rustfs-{index}")))
            .collect();
        assert_eq!(
            Topology::new(Transport::Http, configured),
            Err(ConfigError::UnsupportedPeerCount { actual: count })
        );
    }
}

#[test]
fn duplicate_peer_identity_is_rejected_regardless_of_case_or_port() {
    for port in [9000, 9001] {
        let mut configured = peers();
        configured[2] = Peer::new("RUSTFS-0.RUSTFS", port, drives()).unwrap();
        assert_eq!(
            Topology::new(Transport::Http, configured),
            Err(ConfigError::DuplicatePeer {
                first: 0,
                second: 2
            })
        );
    }
}

#[test]
fn local_identity_must_be_valid_and_in_the_topology() {
    assert_eq!(
        RustFsConfig::new(topology(), "rustfs-4.rustfs"),
        Err(ConfigError::LocalPeerNotFound)
    );
    assert_eq!(
        RustFsConfig::new(topology(), "http://rustfs-0.rustfs"),
        Err(ConfigError::InvalidHost)
    );
}

#[test]
fn unchanged_topology_and_restart_configuration_are_accepted() {
    let original = topology();
    assert_eq!(original.ensure_unchanged(&original.clone()), Ok(()));
    let config = RustFsConfig::new(original.clone(), "rustfs-0.rustfs").unwrap();
    let requested = RustFsConfig::new(original, "RUSTFS-0.RUSTFS").unwrap();
    assert_eq!(config.ensure_restart_compatible(&requested), Ok(()));
}

#[test]
fn peer_reordering_is_not_a_compatible_restart() {
    let mut reordered = peers();
    reordered.swap(0, 1);
    let requested = Topology::new(Transport::Http, reordered).unwrap();
    assert_eq!(
        topology().ensure_unchanged(&requested),
        Err(ConfigError::TopologyChangeUnsupported)
    );
    let current = RustFsConfig::new(topology(), "rustfs-0.rustfs").unwrap();
    let requested = RustFsConfig::new(requested, "rustfs-0.rustfs").unwrap();
    assert_eq!(
        current.ensure_restart_compatible(&requested),
        Err(ConfigError::TopologyChangeUnsupported)
    );
}

#[test]
fn drive_reordering_is_a_topology_change() {
    let mut reordered = drives();
    reordered.swap(0, 1);
    let mut configured = peers();
    configured[0] = Peer::new("rustfs-0.rustfs", 9000, reordered).unwrap();
    let requested = Topology::new(Transport::Http, configured).unwrap();
    assert_eq!(
        topology().ensure_unchanged(&requested),
        Err(ConfigError::TopologyChangeUnsupported)
    );
}

#[test]
fn peer_port_and_drive_replacement_are_topology_changes() {
    let mut changed_drives = drives();
    changed_drives[0] = "/replacement".to_string();
    for changed in [
        peer("replacement.rustfs"),
        Peer::new("rustfs-0.rustfs", 9001, drives()).unwrap(),
        Peer::new("rustfs-0.rustfs", 9000, changed_drives).unwrap(),
    ] {
        let mut configured = peers();
        configured[0] = changed;
        let requested = Topology::new(Transport::Http, configured).unwrap();
        assert_eq!(
            topology().ensure_unchanged(&requested),
            Err(ConfigError::TopologyChangeUnsupported)
        );
    }
}

#[test]
fn transport_changes_require_a_separate_migration() {
    let requested = Topology::new(Transport::Https, peers()).unwrap();
    assert_eq!(
        topology().ensure_unchanged(&requested),
        Err(ConfigError::TopologyChangeUnsupported)
    );
}

#[test]
fn restart_cannot_switch_to_another_local_peer() {
    let current = RustFsConfig::new(topology(), "rustfs-0.rustfs").unwrap();
    let requested = RustFsConfig::new(topology(), "rustfs-1.rustfs").unwrap();
    assert_eq!(
        current.ensure_restart_compatible(&requested),
        Err(ConfigError::LocalPeerChangeUnsupported)
    );
}
