use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use quote::ToTokens;
use syn::visit::Visit;
use syn::{ImplItem, Item, TraitItem, Type};

const CAPABILITY_MARKERS: &[&str] = &[
    "ProcessRuntime",
    "AuthorityRuntime",
    "PeerRuntime",
    "AccessClosure",
    "AccessRuntime",
    "ReportLifecycle",
    "EvidenceRuntime",
    "EffectEvidenceRuntime",
    "BuildLifecycleRuntime",
    "BuildCancellationRuntime",
    "OutboundLifecycleRuntime",
    "RemovalWitnessRuntime",
    "TopologyRuntime",
    "RecoveryRuntime",
    "ReportRuntime",
    "BuildRuntime",
    "BuildAttemptRuntime",
    "PeerDiscoveryRuntime",
    "OutboundRuntime",
    "dynProcessLifecycle",
    "dynAuthorityLifecycle",
    "dynAccessLifecycle",
    "dynBuildLifecycle",
    "dynBuildCancellation",
    "dynTopologyLifecycle",
    "dynLifecycleObservation",
    "dynOutboundLifecycle",
];

const BROAD_OWNER_MARKERS: &[&str] = &[
    "RuntimeHost",
    "PodRuntime",
    "RegisteredReplicator",
    "LifecycleWiring",
    "ReplicatorLifecycleRegistration",
    "ManagedLifecycleBackend",
    "CustomReplicatorHost",
];

fn marker_count(value: &str, markers: &[&str], aliases: &BTreeMap<String, String>) -> usize {
    let identifiers = value
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|identifier| !identifier.is_empty())
        .collect::<BTreeSet<_>>();
    markers
        .iter()
        .filter(|marker| {
            identifiers.contains(**marker)
                || aliases.iter().any(|(alias, target)| {
                    target == **marker && identifiers.contains(alias.as_str())
                })
        })
        .count()
}

fn capability_marker_count(value: &str) -> usize {
    marker_count(value, CAPABILITY_MARKERS, &BTreeMap::new())
}

fn guarded_signature(signature: &syn::Signature, aliases: &BTreeMap<String, String>) -> bool {
    let signature = compact(signature);
    marker_count(&signature, CAPABILITY_MARKERS, aliases) > 0
        || marker_count(&signature, BROAD_OWNER_MARKERS, aliases) > 0
}

fn local_use_aliases(file: &syn::File) -> BTreeMap<String, String> {
    struct UseAliasVisitor {
        aliases: BTreeMap<String, String>,
    }

    fn collect(tree: &syn::UseTree, aliases: &mut BTreeMap<String, String>) {
        match tree {
            syn::UseTree::Path(path) => collect(&path.tree, aliases),
            syn::UseTree::Name(name) => {
                aliases
                    .entry(name.ident.to_string())
                    .or_insert_with(|| name.ident.to_string());
            }
            syn::UseTree::Rename(rename) => {
                aliases
                    .entry(rename.rename.to_string())
                    .or_insert_with(|| rename.ident.to_string());
            }
            syn::UseTree::Group(group) => {
                for tree in &group.items {
                    collect(tree, aliases);
                }
            }
            syn::UseTree::Glob(_) => {}
        }
    }

    impl<'ast> Visit<'ast> for UseAliasVisitor {
        fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
            collect(&item.tree, &mut self.aliases);
            syn::visit::visit_item_use(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            if let Type::Path(path) = item.ty.as_ref()
                && let Some(target) = path.path.segments.last()
            {
                let target = self
                    .aliases
                    .get(&target.ident.to_string())
                    .cloned()
                    .unwrap_or_else(|| target.ident.to_string());
                self.aliases.entry(item.ident.to_string()).or_insert(target);
            }
            syn::visit::visit_item_type(self, item);
        }
    }

    let mut visitor = UseAliasVisitor {
        aliases: BTreeMap::new(),
    };
    visitor.visit_file(file);
    visitor.aliases
}

fn source(path: &str) -> String {
    fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
}

fn production_rust_files(root: &std::path::Path) -> Vec<PathBuf> {
    fn collect(path: &std::path::Path, files: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().and_then(|name| name.to_str()) != Some("tests") {
                    collect(&path, files);
                }
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    collect(root, &mut files);
    files.sort();
    files
}

fn validate_production_module(
    relative: &std::path::Path,
    source: &str,
) -> Result<BTreeSet<String>, String> {
    let display = relative.to_string_lossy();
    if source.contains("ReplicatorLifecycleHost") {
        return Err(format!(
            "{display}: unclassified transition-facade consumer"
        ));
    }
    if matches!(
        relative.to_str(),
        Some("hosting.rs" | "custom.rs" | "lifecycle.rs")
    ) {
        return Ok(BTreeSet::new());
    }
    let file = syn::parse_file(source).map_err(|error| format!("{display}: {error}"))?;
    let allowed_aggregates: &[&str] = match relative.to_str() {
        Some("process.rs") => &["ReplicaHandle"],
        Some("service.rs") => &["AgentService"],
        Some("testing.rs") => &["Endpoint"],
        _ => &[],
    };
    reject_broad_aliases(&file).map_err(|error| format!("{display}: {error}"))?;
    reject_unclassified_capability_aggregates(&file, allowed_aggregates)
        .map_err(|error| format!("{display}: {error}"))?;
    reject_aggregate_signatures(&file).map_err(|error| format!("{display}: {error}"))?;
    Ok(capability_signature_sites(&file)
        .into_iter()
        .map(|site| format!("{display}::{site}"))
        .collect())
}

fn names(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn assert_rejected(result: Result<(), String>, expected: &str) {
    let error = result.expect_err("mutation unexpectedly passed validation");
    assert!(
        error.contains(expected),
        "diagnostic did not identify {expected:?}: {error}"
    );
}

fn type_name(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn compact<T: ToTokens>(value: &T) -> String {
    value.to_token_stream().to_string().replace(' ', "")
}

fn impl_name(item: &syn::ItemImpl) -> String {
    type_name(&item.self_ty).unwrap_or_else(|| "<unknown>".into())
}

fn block_uses_transition_facade<T: ToTokens>(block: &T) -> bool {
    let block = compact(block);
    let mut rest = block.as_str();
    while let Some(index) = rest.find(".lifecycle") {
        let following = rest[index + ".lifecycle".len()..].chars().next();
        if following.is_none_or(|character| character != '_' && !character.is_ascii_alphanumeric())
        {
            return true;
        }
        rest = &rest[index + ".lifecycle".len()..];
    }
    false
}

fn collect_transition_consumers(items: &[Item], module: &str, consumers: &mut BTreeSet<String>) {
    for item in items {
        match item {
            Item::Fn(function) if block_uses_transition_facade(&function.block) => {
                consumers.insert(format!("{module}fn {}", function.sig.ident));
            }
            Item::Impl(item) => {
                let owner = impl_name(item);
                for implementation in &item.items {
                    let ImplItem::Fn(method) = implementation else {
                        continue;
                    };
                    if block_uses_transition_facade(&method.block) {
                        consumers.insert(format!("{module}{owner}::{}", method.sig.ident));
                    }
                }
            }
            Item::Mod(item) => {
                if let Some((_, items)) = &item.content {
                    collect_transition_consumers(
                        items,
                        &format!("{module}{}::", item.ident),
                        consumers,
                    );
                }
            }
            _ => {}
        }
    }
}

fn transition_consumers(source: &str) -> Result<BTreeSet<String>, String> {
    let file = syn::parse_file(source).map_err(|error| format!("parse consumers: {error}"))?;
    let mut consumers = BTreeSet::new();
    collect_transition_consumers(&file.items, "", &mut consumers);
    Ok(consumers)
}

fn struct_fields(file: &syn::File, name: &str) -> Option<BTreeMap<String, String>> {
    for item in &file.items {
        let Item::Struct(item) = item else {
            continue;
        };
        if item.ident != name {
            continue;
        }
        return Some(
            item.fields
                .iter()
                .filter_map(|field| {
                    field
                        .ident
                        .as_ref()
                        .map(|ident| (ident.to_string(), compact(&field.ty)))
                })
                .collect(),
        );
    }
    None
}

struct BroadAliasVisitor {
    issue: Option<String>,
}

impl<'ast> Visit<'ast> for BroadAliasVisitor {
    fn visit_item_type(&mut self, alias: &'ast syn::ItemType) {
        const FORBIDDEN: &[&str] = &[
            "LifecycleWiring",
            "ReplicatorLifecycleHost",
            "RuntimeHost",
            "PodRuntime",
            "ManagedLifecycleBackend",
            "CustomReplicatorHost",
        ];
        let target = compact(&alias.ty);
        if FORBIDDEN.iter().any(|forbidden| target.contains(forbidden)) {
            self.issue = Some(format!("broad lifecycle alias {} -> {target}", alias.ident));
        } else if capability_marker_count(&target) >= 1 {
            self.issue = Some(format!("capability alias {} -> {target}", alias.ident));
        }
        syn::visit::visit_item_type(self, alias);
    }
}

fn reject_broad_aliases(file: &syn::File) -> Result<(), String> {
    let mut visitor = BroadAliasVisitor { issue: None };
    visitor.visit_file(file);
    let aliases = local_use_aliases(file);

    struct GuardedUseAliasVisitor {
        issue: Option<String>,
    }

    fn inspect_use(tree: &syn::UseTree, issue: &mut Option<String>) {
        match tree {
            syn::UseTree::Path(path) => inspect_use(&path.tree, issue),
            syn::UseTree::Rename(rename)
                if CAPABILITY_MARKERS.contains(&rename.ident.to_string().as_str())
                    || BROAD_OWNER_MARKERS.contains(&rename.ident.to_string().as_str()) =>
            {
                *issue = Some(format!(
                    "guarded import alias {} -> {}",
                    rename.rename, rename.ident
                ));
            }
            syn::UseTree::Group(group) => {
                for tree in &group.items {
                    inspect_use(tree, issue);
                }
            }
            _ => {}
        }
    }

    impl<'ast> Visit<'ast> for GuardedUseAliasVisitor {
        fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
            inspect_use(&item.tree, &mut self.issue);
            syn::visit::visit_item_use(self, item);
        }
    }

    let mut guarded_use = GuardedUseAliasVisitor { issue: None };
    guarded_use.visit_file(file);
    if let Some(issue) = guarded_use.issue {
        return Err(issue);
    }

    struct ResolvedAliasVisitor<'a> {
        aliases: &'a BTreeMap<String, String>,
        issue: Option<String>,
    }

    impl<'ast> Visit<'ast> for ResolvedAliasVisitor<'_> {
        fn visit_item_type(&mut self, alias: &'ast syn::ItemType) {
            let target = compact(&alias.ty);
            if marker_count(&target, BROAD_OWNER_MARKERS, self.aliases) > 0 {
                self.issue = Some(format!("broad lifecycle alias {} -> {target}", alias.ident));
            } else if marker_count(&target, CAPABILITY_MARKERS, self.aliases) >= 1 {
                self.issue = Some(format!("capability alias {} -> {target}", alias.ident));
            }
            syn::visit::visit_item_type(self, alias);
        }
    }

    let mut resolved = ResolvedAliasVisitor {
        aliases: &aliases,
        issue: None,
    };
    resolved.visit_file(file);
    if let Some(issue) = resolved.issue {
        return Err(issue);
    }
    match visitor.issue {
        Some(issue) => Err(issue),
        None => Ok(()),
    }
}

fn impl_methods(file: &syn::File, name: &str) -> Option<BTreeSet<String>> {
    let mut found = false;
    let mut methods = BTreeSet::new();
    for item in &file.items {
        let Item::Impl(item) = item else {
            continue;
        };
        if item.trait_.is_some() || type_name(&item.self_ty).as_deref() != Some(name) {
            continue;
        }
        found = true;
        methods.extend(item.items.iter().filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method.sig.ident.to_string()),
            _ => None,
        }));
    }
    found.then_some(methods)
}

fn impl_method<'a>(file: &'a syn::File, owner: &str, method: &str) -> Option<&'a syn::ImplItemFn> {
    for item in &file.items {
        let Item::Impl(item) = item else {
            continue;
        };
        if item.trait_.is_some() || type_name(&item.self_ty).as_deref() != Some(owner) {
            continue;
        }
        for implementation in &item.items {
            let ImplItem::Fn(implementation) = implementation else {
                continue;
            };
            if implementation.sig.ident == method {
                return Some(implementation);
            }
        }
    }
    None
}

fn assert_shape(
    file: &syn::File,
    name: &str,
    expected_fields: &[(&str, &str)],
    expected_methods: &[&str],
) -> Result<(), String> {
    let expected_fields = expected_fields
        .iter()
        .map(|(field, ty)| ((*field).to_owned(), (*ty).to_owned()))
        .collect::<BTreeMap<_, _>>();
    let actual_fields = struct_fields(file, name);
    if actual_fields.as_ref() != Some(&expected_fields) {
        return Err(format!("{name} field budget changed: {actual_fields:#?}"));
    }
    let expected_methods = names(expected_methods);
    let actual_methods = impl_methods(file, name);
    if actual_methods.as_ref() != Some(&expected_methods) {
        return Err(format!("{name} method budget changed: {actual_methods:#?}"));
    }
    Ok(())
}

fn reject_capability_escape_paths(
    file: &syn::File,
    traits: &[&str],
    views: &[&str],
) -> Result<(), String> {
    const FORBIDDEN: &[&str] = &[
        "LifecycleWiring",
        "ReplicatorLifecycleHost",
        "RegisteredReplicator",
        "RuntimeHost",
        "PodRuntime",
        "ManagedLifecycleBackend",
        "CustomReplicatorHost",
    ];
    for item in &file.items {
        match item {
            Item::Trait(item) if traits.contains(&item.ident.to_string().as_str()) => {
                for method in &item.items {
                    let TraitItem::Fn(method) = method else {
                        continue;
                    };
                    let signature = compact(&method.sig);
                    if FORBIDDEN
                        .iter()
                        .any(|forbidden| signature.contains(forbidden))
                    {
                        return Err(format!(
                            "{}::{} exposes a broad type",
                            item.ident, method.sig.ident
                        ));
                    }
                }
            }
            Item::Impl(item)
                if type_name(&item.self_ty)
                    .as_deref()
                    .is_some_and(|name| views.contains(&name)) =>
            {
                if item.trait_.is_some() {
                    return Err(format!(
                        "{} implements an unapproved conversion trait",
                        impl_name(item)
                    ));
                }
                for method in &item.items {
                    let ImplItem::Fn(method) = method else {
                        continue;
                    };
                    let signature = compact(&method.sig);
                    if FORBIDDEN
                        .iter()
                        .any(|forbidden| signature.contains(forbidden))
                    {
                        return Err(format!(
                            "{}::{} exposes a broad type",
                            impl_name(item),
                            method.sig.ident
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn reject_unclassified_capability_aggregates(
    file: &syn::File,
    allowed: &[&str],
) -> Result<(), String> {
    struct AggregateVisitor<'a> {
        allowed: &'a [&'a str],
        aliases: &'a BTreeMap<String, String>,
        depth: usize,
        issue: Option<String>,
    }

    impl<'ast> Visit<'ast> for AggregateVisitor<'_> {
        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            let capabilities = item
                .fields
                .iter()
                .map(|field| marker_count(&compact(&field.ty), CAPABILITY_MARKERS, self.aliases))
                .sum::<usize>();
            let broad_owners = item
                .fields
                .iter()
                .map(|field| marker_count(&compact(&field.ty), BROAD_OWNER_MARKERS, self.aliases))
                .sum::<usize>();
            if capabilities >= 2
                && (self.depth > 0 || !self.allowed.contains(&item.ident.to_string().as_str()))
            {
                self.issue = Some(format!(
                    "{} is an unclassified capability aggregate",
                    item.ident
                ));
            }
            if broad_owners > 0
                && (self.depth > 0 || !self.allowed.contains(&item.ident.to_string().as_str()))
            {
                self.issue = Some(format!("{} retains an unapproved broad owner", item.ident));
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_block(&mut self, block: &'ast syn::Block) {
            self.depth += 1;
            syn::visit::visit_block(self, block);
            self.depth -= 1;
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            self.depth += 1;
            syn::visit::visit_item_mod(self, item);
            self.depth -= 1;
        }
    }

    let aliases = local_use_aliases(file);
    let mut visitor = AggregateVisitor {
        allowed,
        aliases: &aliases,
        depth: 0,
        issue: None,
    };
    visitor.visit_file(file);
    match visitor.issue {
        Some(issue) => Err(issue),
        None => Ok(()),
    }
}

fn reject_aggregate_signatures(file: &syn::File) -> Result<(), String> {
    fn check_signature(
        owner: &str,
        signature: &syn::Signature,
        aliases: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        let signature = compact(signature);
        if marker_count(&signature, CAPABILITY_MARKERS, aliases)
            + marker_count(&signature, BROAD_OWNER_MARKERS, aliases)
            >= 2
        {
            return Err(format!("{owner} exposes a capability aggregate signature"));
        }
        Ok(())
    }

    fn inspect(
        items: &[Item],
        module: &str,
        aliases: &BTreeMap<String, String>,
    ) -> Result<(), String> {
        for item in items {
            match item {
                Item::Fn(item) => check_signature(
                    &format!("{module}fn {}", item.sig.ident),
                    &item.sig,
                    aliases,
                )?,
                Item::Trait(item) => {
                    for method in &item.items {
                        if let TraitItem::Fn(method) = method {
                            check_signature(
                                &format!("{module}{}::{}", item.ident, method.sig.ident),
                                &method.sig,
                                aliases,
                            )?;
                        }
                    }
                }
                Item::Impl(item) => {
                    for method in &item.items {
                        if let ImplItem::Fn(method) = method {
                            check_signature(
                                &format!("{module}{}::{}", impl_name(item), method.sig.ident),
                                &method.sig,
                                aliases,
                            )?;
                        }
                    }
                }
                Item::Mod(item) => {
                    if let Some((_, items)) = &item.content {
                        inspect(items, &format!("{module}{}::", item.ident), aliases)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    let aliases = local_use_aliases(file);
    inspect(&file.items, "", &aliases)
}

fn capability_signature_sites(file: &syn::File) -> BTreeSet<String> {
    struct NestedSignatureVisitor<'a> {
        prefix: &'a str,
        aliases: &'a BTreeMap<String, String>,
        sites: &'a mut BTreeSet<String>,
    }

    impl<'ast> Visit<'ast> for NestedSignatureVisitor<'_> {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if guarded_signature(&item.sig, self.aliases) {
                self.sites
                    .insert(format!("{}fn {}", self.prefix, item.sig.ident));
            }
            syn::visit::visit_item_fn(self, item);
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            for method in &item.items {
                if let TraitItem::Fn(method) = method
                    && guarded_signature(&method.sig, self.aliases)
                {
                    self.sites.insert(format!(
                        "{}{}::{}",
                        self.prefix, item.ident, method.sig.ident
                    ));
                }
            }
            syn::visit::visit_item_trait(self, item);
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            for method in &item.items {
                if let ImplItem::Fn(method) = method
                    && guarded_signature(&method.sig, self.aliases)
                {
                    self.sites.insert(format!(
                        "{}{}::{}",
                        self.prefix,
                        impl_name(item),
                        method.sig.ident
                    ));
                }
            }
            syn::visit::visit_item_impl(self, item);
        }
    }

    fn inspect_nested(
        block: &syn::Block,
        prefix: &str,
        aliases: &BTreeMap<String, String>,
        sites: &mut BTreeSet<String>,
    ) {
        NestedSignatureVisitor {
            prefix,
            aliases,
            sites,
        }
        .visit_block(block);
    }

    fn inspect(
        items: &[Item],
        module: &str,
        aliases: &BTreeMap<String, String>,
        sites: &mut BTreeSet<String>,
    ) {
        for item in items {
            match item {
                Item::Fn(item) => {
                    if guarded_signature(&item.sig, aliases) {
                        sites.insert(format!("{module}fn {}", item.sig.ident));
                    }
                    inspect_nested(
                        &item.block,
                        &format!("{module}fn {}::", item.sig.ident),
                        aliases,
                        sites,
                    );
                }
                Item::Trait(item) => {
                    for method in &item.items {
                        if let TraitItem::Fn(method) = method {
                            if guarded_signature(&method.sig, aliases) {
                                sites.insert(format!(
                                    "{module}{}::{}",
                                    item.ident, method.sig.ident
                                ));
                            }
                            if let Some(block) = &method.default {
                                inspect_nested(
                                    block,
                                    &format!("{module}{}::{}::", item.ident, method.sig.ident),
                                    aliases,
                                    sites,
                                );
                            }
                        }
                    }
                }
                Item::Impl(item) => {
                    for method in &item.items {
                        if let ImplItem::Fn(method) = method {
                            if guarded_signature(&method.sig, aliases) {
                                sites.insert(format!(
                                    "{module}{}::{}",
                                    impl_name(item),
                                    method.sig.ident
                                ));
                            }
                            inspect_nested(
                                &method.block,
                                &format!("{module}{}::{}::", impl_name(item), method.sig.ident),
                                aliases,
                                sites,
                            );
                        }
                    }
                }
                Item::Mod(item) => {
                    if let Some((_, items)) = &item.content {
                        inspect(items, &format!("{module}{}::", item.ident), aliases, sites);
                    }
                }
                _ => {}
            }
        }
    }

    let mut sites = BTreeSet::new();
    let aliases = local_use_aliases(file);
    inspect(&file.items, "", &aliases, &mut sites);
    sites
}

fn validate(lifecycle: &str, custom: &str, hosting: &str) -> Result<(), String> {
    for source in [lifecycle, custom, hosting] {
        if source.contains("ReplicatorLifecycleBackend") {
            return Err("universal lifecycle backend remains".into());
        }
    }

    let lifecycle_file =
        syn::parse_file(lifecycle).map_err(|error| format!("parse lifecycle: {error}"))?;
    let expected_traits = BTreeMap::from([
        (
            "ProcessLifecycle",
            names(&[
                "owns_stream_session",
                "complete_open",
                "complete_close",
                "complete_abort",
                "notify_abort",
                "fence_writes",
                "invalidate_public_access",
                "settle_primary_prefix",
            ]),
        ),
        (
            "AuthorityLifecycle",
            names(&[
                "cancel_configuration_work",
                "restore_authority",
                "admit_authority",
                "register_peer_session",
                "describe_peer",
            ]),
        ),
        (
            "AccessLifecycle",
            names(&[
                "defer_restored_access",
                "run_access_transaction",
                "restored_access",
            ]),
        ),
        (
            "BuildLifecycle",
            names(&[
                "admit_build_authority",
                "retire_build",
                "wait_for_build_completion",
                "build_replica",
                "remove_replica",
                "select_build",
                "execute_build",
                "accept_build",
                "enqueue_build",
                "confirm_build_completion",
            ]),
        ),
        (
            "BuildCancellation",
            names(&[
                "cancel_outbound_build",
                "cancel_outbound_build_attempt",
                "build_generation",
            ]),
        ),
        (
            "TopologyLifecycle",
            names(&[
                "wait_for_catch_up",
                "authorize_failover_prefix",
                "prepare_switchover",
                "prepare_secondary_removal",
                "observe_secondary_removal",
                "observe_secondary_removal_progress",
                "accept_secondary_removal",
                "accept_historical_secondary_removal",
                "fence_retirement",
                "complete_retirement",
                "topology_receipt",
            ]),
        ),
        (
            "LifecycleObservation",
            names(&[
                "refresh_progress",
                "observe_progress",
                "snapshot",
                "postcondition",
            ]),
        ),
        ("OutboundLifecycle", names(&["next_outbound"])),
    ])
    .into_iter()
    .map(|(name, methods)| (name.to_owned(), methods))
    .collect::<BTreeMap<_, _>>();
    let mut actual_traits = BTreeMap::new();
    let mut fixture_methods = BTreeMap::new();
    for item in &lifecycle_file.items {
        let Item::Trait(item) = item else {
            continue;
        };
        let bounds = item
            .supertraits
            .to_token_stream()
            .to_string()
            .replace(' ', "");
        if bounds != "Send+Sync" {
            return Err(format!(
                "{} has unsupported supertraits: {bounds}",
                item.ident
            ));
        }
        let methods = item
            .items
            .iter()
            .filter_map(|item| match item {
                TraitItem::Fn(method) => {
                    if method
                        .attrs
                        .iter()
                        .any(|attribute| attribute.path().is_ident("cfg"))
                    {
                        let cfg = method
                            .attrs
                            .iter()
                            .find(|attribute| attribute.path().is_ident("cfg"))
                            .map(compact)
                            .unwrap();
                        fixture_methods.insert(method.sig.ident.to_string(), cfg);
                    }
                    Some(method.sig.ident.to_string())
                }
                _ => None,
            })
            .collect();
        actual_traits.insert(item.ident.to_string(), methods);
    }
    if actual_traits != expected_traits {
        return Err(format!(
            "capability method budgets changed: {actual_traits:#?}"
        ));
    }
    let fixture_cfg = "#[cfg(any(all(test,kuberic_workspace_tests),feature=\"testing\"))]";
    let expected_fixture_methods = BTreeMap::from([
        ("build_replica".to_owned(), fixture_cfg.to_owned()),
        (
            "invalidate_public_access".to_owned(),
            fixture_cfg.to_owned(),
        ),
        ("remove_replica".to_owned(), fixture_cfg.to_owned()),
    ]);
    if fixture_methods != expected_fixture_methods {
        return Err(format!(
            "fixture-gated capability methods changed: {fixture_methods:#?}"
        ));
    }
    reject_broad_aliases(&lifecycle_file)?;
    assert_shape(
        &lifecycle_file,
        "LifecycleWiring",
        &[
            ("process", "Arc<dynProcessLifecycle>"),
            ("authority", "Arc<dynAuthorityLifecycle>"),
            ("access", "Arc<dynAccessLifecycle>"),
            ("build", "Arc<dynBuildLifecycle>"),
            ("build_cancellation", "Arc<dynBuildCancellation>"),
            ("topology", "Arc<dynTopologyLifecycle>"),
            ("observation", "Arc<dynLifecycleObservation>"),
            ("outbound", "Arc<dynOutboundLifecycle>"),
        ],
        &[
            "new",
            "process_runtime",
            "authority_runtime",
            "peer_runtime",
            "access_closure",
            "access_runtime",
            "report_lifecycle",
            "evidence_runtime",
            "effect_evidence_runtime",
            "build_runtime",
            "build_cancellation",
            "outbound_runtime",
            "removal_witness_runtime",
            "topology_runtime",
            "recovery_runtime",
        ],
    )?;
    assert_shape(
        &lifecycle_file,
        "ProcessRuntime",
        &[("inner", "Arc<dynProcessLifecycle>")],
        &[
            "owns_stream_session",
            "complete_open",
            "complete_close",
            "complete_abort",
            "notify_abort",
            "fence_writes",
            "invalidate_public_access",
            "settle_primary_prefix",
        ],
    )?;
    assert_shape(
        &lifecycle_file,
        "AuthorityRuntime",
        &[("inner", "Arc<dynAuthorityLifecycle>")],
        &["cancel_configuration_work", "admit_authority"],
    )?;
    assert_shape(
        &lifecycle_file,
        "PeerRuntime",
        &[("inner", "Arc<dynAuthorityLifecycle>")],
        &["register_peer_session", "describe_peer"],
    )?;
    assert_shape(
        &lifecycle_file,
        "AccessClosure",
        &[("inner", "Arc<dynAccessLifecycle>")],
        &["set_access"],
    )?;
    assert_shape(
        &lifecycle_file,
        "EvidenceRuntime",
        &[("inner", "Arc<dynLifecycleObservation>")],
        &["snapshot"],
    )?;
    assert_shape(
        &lifecycle_file,
        "AccessRuntime",
        &[("inner", "Arc<dynAccessLifecycle>")],
        &["begin_effect"],
    )?;
    assert_shape(
        &lifecycle_file,
        "ReportLifecycle",
        &[
            ("access", "Arc<dynAccessLifecycle>"),
            ("observation", "Arc<dynLifecycleObservation>"),
        ],
        &["observe_progress", "reconcile_access", "snapshot"],
    )?;
    assert_shape(
        &lifecycle_file,
        "EffectEvidenceRuntime",
        &[("inner", "Arc<dynLifecycleObservation>")],
        &["refresh_progress", "postcondition"],
    )?;
    assert_shape(
        &lifecycle_file,
        "BuildLifecycleRuntime",
        &[
            ("inner", "Arc<dynBuildLifecycle>"),
            ("cancellation", "Arc<dynBuildCancellation>"),
            ("managed", "bool"),
        ],
        &[
            "is_managed",
            "admit_authority",
            "retire",
            "cancel",
            "wait_for_completion",
            "build_replica",
            "remove_replica",
            "select",
            "execute",
            "accept",
            "enqueue",
            "confirm",
        ],
    )?;
    assert_shape(
        &lifecycle_file,
        "BuildCancellationRuntime",
        &[("inner", "Arc<dynBuildCancellation>")],
        &["generation", "cancel_attempt"],
    )?;
    assert_shape(
        &lifecycle_file,
        "OutboundLifecycleRuntime",
        &[("inner", "Arc<dynOutboundLifecycle>")],
        &["next"],
    )?;
    assert_shape(
        &lifecycle_file,
        "RemovalWitnessRuntime",
        &[("inner", "Arc<dynTopologyLifecycle>")],
        &["observe"],
    )?;
    assert_shape(
        &lifecycle_file,
        "TopologyRuntime",
        &[("inner", "Arc<dynTopologyLifecycle>")],
        &[
            "wait_for_catch_up",
            "authorize_failover_prefix",
            "prepare_switchover",
            "prepare_secondary_removal",
            "observe_secondary_removal",
            "observe_secondary_removal_progress",
            "accept_secondary_removal",
            "accept_historical_secondary_removal",
            "fence_retirement",
            "complete_retirement",
            "receipt",
        ],
    )?;
    assert_shape(
        &lifecycle_file,
        "RecoveryRuntime",
        &[
            ("authority", "Arc<dynAuthorityLifecycle>"),
            ("access", "Arc<dynAccessLifecycle>"),
            ("topology", "Arc<dynTopologyLifecycle>"),
            ("observation", "Arc<dynLifecycleObservation>"),
        ],
        &[
            "restore_authority",
            "snapshot",
            "restore_access",
            "accept_secondary_removal",
            "accept_historical_secondary_removal",
        ],
    )?;
    reject_capability_escape_paths(
        &lifecycle_file,
        &[
            "ProcessLifecycle",
            "AuthorityLifecycle",
            "AccessLifecycle",
            "BuildLifecycle",
            "BuildCancellation",
            "TopologyLifecycle",
            "LifecycleObservation",
            "OutboundLifecycle",
        ],
        &[
            "ProcessRuntime",
            "AuthorityRuntime",
            "PeerRuntime",
            "AccessClosure",
            "AccessRuntime",
            "ReportLifecycle",
            "EvidenceRuntime",
            "EffectEvidenceRuntime",
            "BuildLifecycleRuntime",
            "BuildCancellationRuntime",
            "OutboundLifecycleRuntime",
            "RemovalWitnessRuntime",
            "TopologyRuntime",
            "RecoveryRuntime",
        ],
    )?;
    reject_unclassified_capability_aggregates(
        &lifecycle_file,
        &[
            "LifecycleWiring",
            "ReportLifecycle",
            "BuildLifecycleRuntime",
            "RecoveryRuntime",
        ],
    )?;
    reject_aggregate_signatures(&lifecycle_file)?;

    let custom_file =
        syn::parse_file(custom).map_err(|error| format!("parse custom host: {error}"))?;
    reject_broad_aliases(&custom_file)?;
    if custom.contains("ReplicatorLifecycleHost")
        || impl_methods(&custom_file, "ReplicatorLifecycleHost").is_some()
        || struct_fields(&custom_file, "ReplicatorLifecycleHost").is_some()
    {
        return Err("universal lifecycle facade remains".into());
    }
    let registration_methods = impl_methods(&custom_file, "ReplicatorLifecycleRegistration");
    if registration_methods.as_ref() != Some(&names(&["managed", "service", "from_wiring"])) {
        return Err(format!(
            "ReplicatorLifecycleRegistration methods changed: {registration_methods:#?}"
        ));
    }
    let expected_registration_fields = BTreeMap::from([
        (
            "process".to_owned(),
            "super::lifecycle::ProcessRuntime".to_owned(),
        ),
        (
            "authority".to_owned(),
            "super::lifecycle::AuthorityRuntime".to_owned(),
        ),
        (
            "peer".to_owned(),
            "super::lifecycle::PeerRuntime".to_owned(),
        ),
        (
            "access_closure".to_owned(),
            "super::lifecycle::AccessClosure".to_owned(),
        ),
        (
            "access".to_owned(),
            "super::lifecycle::AccessRuntime".to_owned(),
        ),
        (
            "report".to_owned(),
            "super::lifecycle::ReportLifecycle".to_owned(),
        ),
        (
            "evidence".to_owned(),
            "super::lifecycle::EvidenceRuntime".to_owned(),
        ),
        (
            "effect_evidence".to_owned(),
            "super::lifecycle::EffectEvidenceRuntime".to_owned(),
        ),
        (
            "build".to_owned(),
            "super::lifecycle::BuildLifecycleRuntime".to_owned(),
        ),
        (
            "build_cancellation".to_owned(),
            "super::lifecycle::BuildCancellationRuntime".to_owned(),
        ),
        (
            "outbound".to_owned(),
            "super::lifecycle::OutboundLifecycleRuntime".to_owned(),
        ),
        (
            "removal_witness".to_owned(),
            "super::lifecycle::RemovalWitnessRuntime".to_owned(),
        ),
        (
            "topology".to_owned(),
            "super::lifecycle::TopologyRuntime".to_owned(),
        ),
        (
            "recovery".to_owned(),
            "super::lifecycle::RecoveryRuntime".to_owned(),
        ),
    ]);
    if struct_fields(&custom_file, "ReplicatorLifecycleRegistration").as_ref()
        != Some(&expected_registration_fields)
    {
        return Err("ReplicatorLifecycleRegistration retained complete wiring".into());
    }
    reject_unclassified_capability_aggregates(
        &custom_file,
        &[
            "ReplicatorLifecycleRegistration",
            "AcceptedAccessEffect",
            "ManagedLifecycleBackend",
            "CustomReplicatorHost",
        ],
    )?;
    reject_aggregate_signatures(&custom_file)?;
    let expected_queue_fields = BTreeMap::from([
        ("generation".to_owned(), "u64".to_owned()),
        (
            "decision".to_owned(),
            "Option<oneshot::Sender<bool>>".to_owned(),
        ),
        (
            "completion".to_owned(),
            "tokio::task::JoinHandle<()>".to_owned(),
        ),
    ]);
    if struct_fields(&custom_file, "BuildQueueAdmission").as_ref() != Some(&expected_queue_fields) {
        return Err("build queue cancellation capture changed".into());
    }
    let queue_cancellation = impl_method(&custom_file, "BuildQueueAdmission", "new")
        .ok_or_else(|| "BuildQueueAdmission::new missing".to_owned())?;
    if !compact(&queue_cancellation.sig).contains("build:Weak<dynBuildCancellation>") {
        return Err("build queue cancellation no longer receives a weak build owner".into());
    }

    let hosting_file =
        syn::parse_file(hosting).map_err(|error| format!("parse hosting: {error}"))?;
    reject_broad_aliases(&hosting_file)?;
    let report_host = hosting_file
        .items
        .iter()
        .find_map(|item| match item {
            Item::Trait(item) if item.ident == "ReportHost" => Some(item),
            _ => None,
        })
        .ok_or_else(|| "ReportHost missing".to_owned())?;
    if compact(&report_host.supertraits) != "Send+Sync"
        || report_host
            .items
            .iter()
            .filter_map(|item| match item {
                TraitItem::Fn(method) => Some(method.sig.ident.to_string()),
                _ => None,
            })
            .collect::<BTreeSet<_>>()
            != names(&[
                "observe_progress",
                "snapshot",
                "reconcile_access",
                "partition_report",
                "catch_up_capability",
            ])
    {
        return Err("report host capability budget changed".into());
    }
    assert_shape(
        &hosting_file,
        "ReportRuntime",
        &[("inner", "Arc<dynReportHost>")],
        &[
            "observe_progress",
            "snapshot",
            "reconcile_durable_access",
            "partition_report",
            "catch_up_capability",
        ],
    )?;
    for (trait_name, expected_methods) in [
        (
            "BuildHost",
            names(&[
                "is_managed",
                "describe_peer",
                "snapshot",
                "register_peer_session",
                "authorize_build",
                "execute_build",
                "accept_build",
                "prepare_copy",
                "accept_copy_acknowledgement",
                "accept_acknowledgement",
            ]),
        ),
        ("BuildAttemptHost", names(&["generation", "cancel_attempt"])),
        (
            "PeerDiscoveryHost",
            names(&[
                "snapshot",
                "register_peer_session",
                "observe_secondary_removal_witness",
                "accept_acknowledgement",
                "repair_peer",
            ]),
        ),
        ("OutboundHost", names(&["next_outbound", "snapshot"])),
    ] {
        let item = hosting_file
            .items
            .iter()
            .find_map(|item| match item {
                Item::Trait(item) if item.ident == trait_name => Some(item),
                _ => None,
            })
            .ok_or_else(|| format!("{trait_name} missing"))?;
        let methods = item
            .items
            .iter()
            .filter_map(|item| match item {
                TraitItem::Fn(method) => Some(method.sig.ident.to_string()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if compact(&item.supertraits) != "Send+Sync" || methods != expected_methods {
            return Err(format!("{trait_name} capability budget changed"));
        }
    }
    assert_shape(
        &hosting_file,
        "BuildRuntime",
        &[
            ("inner", "Arc<dynBuildHost>"),
            ("cancellation", "BuildAttemptRuntime"),
        ],
        &[
            "describe_peer",
            "generation",
            "cancellation",
            "snapshot",
            "register_peer_session",
            "authorize_build",
            "prepare_copy",
            "accept_copy_acknowledgement",
            "accept_acknowledgement",
            "execute_admitted_build",
        ],
    )?;
    assert_shape(
        &hosting_file,
        "BuildAttemptRuntime",
        &[("inner", "Arc<dynBuildAttemptHost>")],
        &["generation", "cancel_attempt"],
    )?;
    assert_shape(
        &hosting_file,
        "BuildRuntimeCancellation",
        &[
            (
                "decision",
                "Option<tokio::sync::oneshot::Sender<BuildCancellationDecision>>",
            ),
            ("completion", "tokio::task::JoinHandle<Result<()>>"),
        ],
        &["new", "finish"],
    )?;
    assert_shape(
        &hosting_file,
        "PeerDiscoveryRuntime",
        &[("inner", "Arc<dynPeerDiscoveryHost>")],
        &[
            "snapshot",
            "register_peer_session",
            "observe_secondary_removal_witness",
            "accept_acknowledgement",
            "repair_peer",
        ],
    )?;
    assert_shape(
        &hosting_file,
        "OutboundRuntime",
        &[("inner", "Arc<dynOutboundHost>")],
        &["next_outbound", "snapshot"],
    )?;
    reject_capability_escape_paths(
        &hosting_file,
        &[
            "ReportHost",
            "BuildHost",
            "BuildAttemptHost",
            "PeerDiscoveryHost",
            "OutboundHost",
        ],
        &[
            "ReportRuntime",
            "BuildRuntime",
            "BuildAttemptRuntime",
            "PeerDiscoveryRuntime",
            "OutboundRuntime",
        ],
    )?;
    reject_unclassified_capability_aggregates(
        &hosting_file,
        &[
            "RegisteredReplicator",
            "HostedPrimaryReplicator",
            "BuildRuntime",
            "PodRuntime",
            "RuntimeDataPlane",
            "RuntimeHost",
            "HostAccessView",
            "OpenAttempt",
        ],
    )?;
    reject_aggregate_signatures(&hosting_file)?;
    let signature_sites = capability_signature_sites(&lifecycle_file)
        .into_iter()
        .map(|site| format!("lifecycle::{site}"))
        .chain(
            capability_signature_sites(&custom_file)
                .into_iter()
                .map(|site| format!("custom::{site}")),
        )
        .chain(
            capability_signature_sites(&hosting_file)
                .into_iter()
                .map(|site| format!("hosting::{site}")),
        )
        .collect::<BTreeSet<_>>();
    let expected_signature_sites = names(&[
        "custom::AcceptedAccessTransaction::into_effect",
        "custom::BuildQueueAdmission::new",
        "custom::CustomReplicatorHost::active_host",
        "custom::CustomReplicatorHost::bind_build_cancellation",
        "custom::CustomReplicatorHost::host",
        "custom::CustomReplicatorHost::new",
        "custom::ReplicatorLifecycleRegistration::from_wiring",
        "custom::ReplicatorLifecycleRegistration::managed",
        "custom::ReplicatorLifecycleRegistration::service",
        "hosting::BuildRuntime::cancellation",
        "hosting::BuildRuntimeCancellation::new",
        "hosting::PodRuntime::build_attempt_runtime",
        "hosting::PodRuntime::build_runtime",
        "hosting::PodRuntime::outbound_runtime",
        "hosting::PodRuntime::peer_discovery_runtime",
        "hosting::PodRuntime::report_runtime",
        "hosting::RegisteredReplicator::access_closure",
        "hosting::RegisteredReplicator::access_lifecycle",
        "hosting::RegisteredReplicator::authority_lifecycle",
        "hosting::RegisteredReplicator::build_cancellation",
        "hosting::RegisteredReplicator::build_lifecycle",
        "hosting::RegisteredReplicator::effect_evidence",
        "hosting::RegisteredReplicator::lifecycle_evidence",
        "hosting::RegisteredReplicator::outbound_lifecycle",
        "hosting::RegisteredReplicator::peer_lifecycle",
        "hosting::RegisteredReplicator::process_lifecycle",
        "hosting::RegisteredReplicator::recovery_lifecycle",
        "hosting::RegisteredReplicator::removal_witness",
        "hosting::RegisteredReplicator::report_lifecycle",
        "hosting::RegisteredReplicator::topology_lifecycle",
        "hosting::RuntimeHost::access_closure",
        "hosting::RuntimeHost::authority_lifecycle",
        "hosting::RuntimeHost::build_cancellation",
        "hosting::RuntimeHost::build_lifecycle",
        "hosting::RuntimeHost::lifecycle_evidence",
        "hosting::RuntimeHost::peer_lifecycle",
        "hosting::RuntimeHost::process_lifecycle",
        "hosting::RuntimeHost::recovery_lifecycle",
        "hosting::RuntimeHost::topology_lifecycle",
        "lifecycle::LifecycleWiring::access_closure",
        "lifecycle::LifecycleWiring::access_runtime",
        "lifecycle::LifecycleWiring::authority_runtime",
        "lifecycle::LifecycleWiring::build_cancellation",
        "lifecycle::LifecycleWiring::build_runtime",
        "lifecycle::LifecycleWiring::effect_evidence_runtime",
        "lifecycle::LifecycleWiring::evidence_runtime",
        "lifecycle::LifecycleWiring::outbound_runtime",
        "lifecycle::LifecycleWiring::peer_runtime",
        "lifecycle::LifecycleWiring::process_runtime",
        "lifecycle::LifecycleWiring::recovery_runtime",
        "lifecycle::LifecycleWiring::removal_witness_runtime",
        "lifecycle::LifecycleWiring::report_lifecycle",
        "lifecycle::LifecycleWiring::topology_runtime",
        "lifecycle::fn begin_access_effect",
        "lifecycle::fn commit_access",
        "lifecycle::fn restore_access",
    ]);
    if signature_sites != expected_signature_sites {
        return Err(format!(
            "capability signature inventory changed: {signature_sites:#?}"
        ));
    }
    let registered_fields = struct_fields(&hosting_file, "RegisteredReplicator")
        .ok_or_else(|| "RegisteredReplicator missing".to_owned())?;
    let expected_registered_fields = BTreeMap::from([
        ("control".to_owned(), "Arc<dynReplicator>".to_owned()),
        (
            "primary".to_owned(),
            "Option<Arc<dynPrimaryReplicator>>".to_owned(),
        ),
        (
            "provider".to_owned(),
            "Option<Arc<dynStateProvider>>".to_owned(),
        ),
        (
            "process_lifecycle".to_owned(),
            "Option<lifecycle::ProcessRuntime>".to_owned(),
        ),
        (
            "authority_lifecycle".to_owned(),
            "Option<lifecycle::AuthorityRuntime>".to_owned(),
        ),
        (
            "peer_lifecycle".to_owned(),
            "Option<lifecycle::PeerRuntime>".to_owned(),
        ),
        (
            "access_closure".to_owned(),
            "Option<lifecycle::AccessClosure>".to_owned(),
        ),
        (
            "access_lifecycle".to_owned(),
            "Option<lifecycle::AccessRuntime>".to_owned(),
        ),
        (
            "report_lifecycle".to_owned(),
            "Option<lifecycle::ReportLifecycle>".to_owned(),
        ),
        (
            "lifecycle_evidence".to_owned(),
            "Option<lifecycle::EvidenceRuntime>".to_owned(),
        ),
        (
            "effect_evidence".to_owned(),
            "Option<lifecycle::EffectEvidenceRuntime>".to_owned(),
        ),
        (
            "build_lifecycle".to_owned(),
            "Option<lifecycle::BuildLifecycleRuntime>".to_owned(),
        ),
        (
            "build_cancellation".to_owned(),
            "Option<lifecycle::BuildCancellationRuntime>".to_owned(),
        ),
        (
            "outbound_lifecycle".to_owned(),
            "Option<lifecycle::OutboundLifecycleRuntime>".to_owned(),
        ),
        (
            "removal_witness".to_owned(),
            "Option<lifecycle::RemovalWitnessRuntime>".to_owned(),
        ),
        (
            "topology_lifecycle".to_owned(),
            "Option<lifecycle::TopologyRuntime>".to_owned(),
        ),
        (
            "recovery_lifecycle".to_owned(),
            "Option<lifecycle::RecoveryRuntime>".to_owned(),
        ),
        (
            "managed_data_plane".to_owned(),
            "Option<Arc<dynManagedReplicatorDataPlane>>".to_owned(),
        ),
    ]);
    if registered_fields != expected_registered_fields {
        return Err(format!(
            "RegisteredReplicator field inventory changed: {registered_fields:#?}"
        ));
    }
    let registered_methods = impl_methods(&hosting_file, "RegisteredReplicator");
    if registered_methods.as_ref()
        != Some(&names(&[
            "managed_data_plane",
            "process_lifecycle",
            "authority_lifecycle",
            "peer_lifecycle",
            "access_closure",
            "access_lifecycle",
            "report_lifecycle",
            "lifecycle_evidence",
            "effect_evidence",
            "build_lifecycle",
            "build_cancellation",
            "outbound_lifecycle",
            "removal_witness",
            "topology_lifecycle",
            "recovery_lifecycle",
            "primary",
            "open",
            "change_role",
            "update_epoch",
            "close",
            "current_progress",
            "catch_up_capability",
            "abort",
        ]))
    {
        return Err(format!(
            "RegisteredReplicator methods changed: {registered_methods:#?}"
        ));
    }
    let exact_cancellation = impl_method(&hosting_file, "BuildRuntimeCancellation", "new")
        .ok_or_else(|| "BuildRuntimeCancellation::new missing".to_owned())?;
    let cancellation_signature = compact(&exact_cancellation.sig);
    if !cancellation_signature.contains("runtime:BuildAttemptRuntime")
        || cancellation_signature.contains("runtime:BuildRuntime")
    {
        return Err("build runtime cancellation constructor is not cancellation-only".into());
    }
    let cancellation_body = compact(&exact_cancellation.block);
    if cancellation_body.contains("RuntimeHost")
        || cancellation_body.contains("PodRuntime")
        || cancellation_body.contains(".lifecycle")
    {
        return Err("build runtime cancellation captured a broad owner".into());
    }

    let actual_consumers = transition_consumers(hosting)?
        .into_iter()
        .chain(transition_consumers(custom)?)
        .collect::<BTreeSet<_>>();
    let expected_consumers = BTreeSet::new();
    if actual_consumers != expected_consumers {
        return Err(format!(
            "universal lifecycle consumer remains: {actual_consumers:#?}"
        ));
    }

    Ok(())
}

#[test]
fn lifecycle_capabilities_have_bounded_method_and_consumer_budgets() {
    let lifecycle = source("src/host/lifecycle.rs");
    let custom = source("src/host/custom.rs");
    let hosting = source("src/host/hosting.rs");
    let report = source("src/host/report.rs");
    let transport = source("src/host/transport.rs");
    validate(&lifecycle, &custom, &hosting).unwrap();

    assert!(hosting.contains("#[path = \"lifecycle.rs\"]\nmod lifecycle;"));
    assert!(
        report.contains("use crate::host::hosting::ReportRuntime;")
            && !report.contains("use crate::host::hosting::PodRuntime;")
            && report.contains("report(&self, runtime: &ReportRuntime)")
    );
    let transport_file = syn::parse_file(&transport).unwrap();
    assert_eq!(
        struct_fields(&transport_file, "GrpcOutboundDispatcher")
            .unwrap()
            .get("runtime")
            .map(String::as_str),
        Some("BuildRuntime")
    );
    let dispatch_cancellation =
        impl_method(&transport_file, "BuildDispatchCancellation", "new").unwrap();
    assert!(compact(&dispatch_cancellation.sig).contains("runtime:BuildAttemptRuntime"));
    let peer_reporter = transport_file
        .items
        .iter()
        .find_map(|item| match item {
            Item::Trait(item) if item.ident == "PeerReporter" => Some(item),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        peer_reporter
            .items
            .iter()
            .filter_map(|item| match item {
                TraitItem::Fn(method) => Some(method.sig.ident.to_string()),
                _ => None,
            })
            .collect::<BTreeSet<_>>(),
        names(&["peer_report"])
    );
    assert!(transport.contains("dispatcher: Arc<dyn PeerReporter>"));
    assert_eq!(
        transport.matches("PodRuntime").count(),
        2,
        "only the cfg(test) cancellation helper may retain PodRuntime"
    );
    let host_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/host");
    let mut module_signature_sites = BTreeSet::new();
    for path in production_rust_files(&host_root) {
        let relative = path.strip_prefix(&host_root).unwrap();
        let source = fs::read_to_string(&path).unwrap();
        module_signature_sites.extend(validate_production_module(relative, &source).unwrap());
    }
    let expected_module_signature_sites = names(&[
        "report.rs::AgentReporter::report",
        "service.rs::AgentService::new",
        "testing.rs::InProcessTransport::register",
        "testing.rs::fn admit_lifecycle_authority",
        "testing.rs::fn build_generation",
        "testing.rs::fn cancel_build_attempt",
        "testing.rs::fn close_lifecycle",
        "testing.rs::fn describe_peer",
        "testing.rs::fn execute_build",
        "testing.rs::fn execute_build_with_copy",
        "testing.rs::fn outbound",
        "testing.rs::fn register_lifecycle_peer_session",
        "testing.rs::fn set_lifecycle_access",
        "testing.rs::fn wait_for_lifecycle_catch_up",
        "transport.rs::BuildDispatchCancellation::new",
        "transport.rs::GrpcOutboundDispatcher::new",
        "transport.rs::fn deliver_outbound_with_runtime",
        "transport.rs::fn dispatch_queued_with_retry",
        "transport.rs::fn queued_matches_runtime_authority",
        "transport.rs::fn run_outbound",
        "transport.rs::fn run_peer_discovery",
        "transport.rs::fn spawn_delivery_worker",
        "transport.rs::fn spawn_outbound_worker",
        "transport.rs::fn testing_cancel_build_dispatch",
    ]);
    assert_eq!(module_signature_sites, expected_module_signature_sites);
}

#[test]
fn lifecycle_capability_guard_rejects_broadening_mutations() {
    let lifecycle = source("src/host/lifecycle.rs");
    let custom = source("src/host/custom.rs");
    let hosting = source("src/host/hosting.rs");

    let broad_process = lifecycle.replace(
        "    async fn settle_primary_prefix(&self) -> Result<()>;\n",
        "    async fn settle_primary_prefix(&self) -> Result<()>;\n    async fn execute_build(&self, replica: ReplicaInformation) -> Result<Option<BuildAdmission>>;\n",
    );
    assert_rejected(validate(&broad_process, &custom, &hosting), "execute_build");

    let universal =
        format!("{lifecycle}\ntrait UniversalLifecycle: ProcessLifecycle + BuildLifecycle {{}}\n");
    assert_rejected(
        validate(&universal, &custom, &hosting),
        "UniversalLifecycle",
    );

    let broad_facade = format!("{custom}\nstruct ReplicatorLifecycleHost;\n");
    assert_rejected(
        validate(&lifecycle, &broad_facade, &hosting),
        "universal lifecycle facade",
    );

    let missing_peer_view =
        hosting.replace("    peer_lifecycle: Option<lifecycle::PeerRuntime>,\n", "");
    assert_rejected(
        validate(&lifecycle, &custom, &missing_peer_view),
        "RegisteredReplicator",
    );

    let broad_alias = format!("{lifecycle}\ntype Everything = LifecycleWiring;\n");
    assert_rejected(validate(&broad_alias, &custom, &hosting), "Everything");

    let universal_getter = custom.replace(
        "impl ReplicatorLifecycleRegistration {\n",
        "impl ReplicatorLifecycleRegistration {\n    fn all_capabilities(&self) -> &LifecycleWiring { unreachable!() }\n",
    );
    assert_rejected(
        validate(&lifecycle, &universal_getter, &hosting),
        "all_capabilities",
    );

    let broad_cancellation = hosting.replace(
        "struct BuildRuntimeCancellation {\n",
        "struct BuildRuntimeCancellation {\n    host: Arc<RuntimeHost>,\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &broad_cancellation),
        "BuildRuntimeCancellation",
    );

    let new_facade_consumer = hosting.replace(
        "impl PodRuntime {\n",
        "impl PodRuntime {\n    async fn leaked_lifecycle(&self) { let _ = self.host.lifecycle(); }\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &new_facade_consumer),
        "leaked_lifecycle",
    );

    let universal_view_getter = lifecycle.replace(
        "impl ProcessRuntime {\n",
        "impl ProcessRuntime {\n    fn build_capability(&self) -> Arc<dyn BuildLifecycle> { unreachable!() }\n",
    );
    assert_rejected(
        validate(&universal_view_getter, &custom, &hosting),
        "build_capability",
    );

    let broad_view_field = lifecycle.replace(
        "pub(super) struct ProcessRuntime {\n",
        "pub(super) struct ProcessRuntime {\n    build: Arc<dyn BuildLifecycle>,\n",
    );
    assert_rejected(
        validate(&broad_view_field, &custom, &hosting),
        "ProcessRuntime",
    );

    let local_alias = lifecycle.replace(
        "    pub(super) async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {\n",
        "    pub(super) async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {\n        type AllCapabilities = LifecycleWiring;\n",
    );
    assert_rejected(validate(&local_alias, &custom, &hosting), "AllCapabilities");

    let always_enabled_fixture = lifecycle.replacen(
        "#[cfg(any(all(test, kuberic_workspace_tests), feature = \"testing\"))]",
        "#[cfg(all())]",
        1,
    );
    assert_rejected(
        validate(&always_enabled_fixture, &custom, &hosting),
        "fixture-gated",
    );

    let nested_consumer = format!(
        "{hosting}\nmod leaked {{ async fn consume(host: &RuntimeHost) {{ let _ = host.lifecycle(); }} }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &nested_consumer),
        "leaked::fn consume",
    );

    let direct_field_consumer = hosting.replace(
        "impl PodRuntime {\n",
        "impl PodRuntime {\n    async fn leaked_field(&self) { let registered = self.host.registered.get().unwrap(); let _ = registered.lifecycle.snapshot().await; }\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &direct_field_consumer),
        "leaked_field",
    );

    let broad_closure_capture = hosting.replace(
        "    fn new(runtime: BuildAttemptRuntime, build_id: OperationId, generation: u64) -> Self {\n",
        "    fn new(runtime: BuildAttemptRuntime, build_id: OperationId, generation: u64) -> Self {\n        let broad_host: Option<Arc<RuntimeHost>> = None;\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &broad_closure_capture),
        "build runtime cancellation",
    );

    let retained_wiring = custom.replace(
        "pub(super) struct ReplicatorLifecycleRegistration {\n",
        "pub(super) struct ReplicatorLifecycleRegistration {\n    pub(super) wiring: LifecycleWiring,\n",
    );
    assert_rejected(
        validate(&lifecycle, &retained_wiring, &hosting),
        "ReplicatorLifecycleRegistration",
    );

    let second_view_impl = format!(
        "{lifecycle}\nimpl ProcessRuntime {{ fn build_capability(&self) -> Arc<dyn BuildLifecycle> {{ unreachable!() }} }}\n"
    );
    assert_rejected(
        validate(&second_view_impl, &custom, &hosting),
        "build_capability",
    );

    let qualified_local_alias = lifecycle.replace(
        "    pub(super) async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {\n",
        "    pub(super) async fn set_access(&self, read: AccessStatus, write: AccessStatus) -> Result<()> {\n        type AllCapabilities = super::lifecycle::LifecycleWiring;\n",
    );
    assert_rejected(
        validate(&qualified_local_alias, &custom, &hosting),
        "AllCapabilities",
    );

    let borrowed_facade_field = hosting.replace(
        "impl PodRuntime {\n",
        "impl PodRuntime {\n    fn leaked_field(&self) { let registered = self.host.registered.get().unwrap(); let _borrowed = &registered.lifecycle; }\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &borrowed_facade_field),
        "leaked_field",
    );

    let build_deref = format!(
        "{hosting}\nimpl std::ops::Deref for BuildRuntime {{ type Target = PodRuntime; fn deref(&self) -> &Self::Target {{ unreachable!() }} }}\n"
    );
    assert_rejected(validate(&lifecycle, &custom, &build_deref), "BuildRuntime");

    let broad_peer_signature = hosting.replace(
        "impl PeerDiscoveryRuntime {\n    pub(crate) async fn snapshot(&self) -> RuntimeSnapshot {\n        self.inner.snapshot().await\n    }\n",
        "impl PeerDiscoveryRuntime {\n    pub(crate) async fn snapshot(&self) -> Arc<RuntimeHost> { unreachable!() }\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &broad_peer_signature),
        "PeerDiscoveryRuntime::snapshot",
    );

    let broad_coordinator_cleanup = hosting.replace(
        "    fn new(runtime: BuildAttemptRuntime, build_id: OperationId, generation: u64) -> Self {\n",
        "    fn new(runtime: BuildRuntime, build_id: OperationId, generation: u64) -> Self {\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &broad_coordinator_cleanup),
        "build runtime cancellation",
    );

    let concrete_backend_signature = hosting.replace(
        "impl PeerDiscoveryRuntime {\n    pub(crate) async fn snapshot(&self) -> RuntimeSnapshot {\n",
        "impl PeerDiscoveryRuntime {\n    pub(crate) async fn snapshot(&self) -> Arc<custom::CustomReplicatorHost> {\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &concrete_backend_signature),
        "PeerDiscoveryRuntime::snapshot",
    );

    let universal_registration_getter = hosting.replace(
        "impl RegisteredReplicator {\n",
        "impl RegisteredReplicator {\n    fn all_capabilities(&self) -> (&lifecycle::ProcessRuntime, &lifecycle::TopologyRuntime) { unreachable!() }\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &universal_registration_getter),
        "all_capabilities",
    );

    let renamed_wiring = format!(
        "{hosting}\nstruct LifecycleBroker {{ process: lifecycle::ProcessRuntime, authority: lifecycle::AuthorityRuntime, topology: lifecycle::TopologyRuntime }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &renamed_wiring),
        "LifecycleBroker",
    );

    let host_view_broker = format!(
        "{hosting}\nstruct HostViewBroker {{ build: BuildRuntime, report: ReportRuntime, outbound: OutboundRuntime }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &host_view_broker),
        "HostViewBroker",
    );

    let nested_broker = format!(
        "{hosting}\nmod leaked {{ struct Broker {{ process: lifecycle::ProcessRuntime, topology: lifecycle::TopologyRuntime }} }}\n"
    );
    assert_rejected(validate(&lifecycle, &custom, &nested_broker), "Broker");

    let tuple_alias = format!(
        "{hosting}\ntype CapabilityTuple = (BuildRuntime, ReportRuntime, OutboundRuntime);\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &tuple_alias),
        "CapabilityTuple",
    );

    let tuple_getter = format!(
        "{hosting}\nfn all_views() -> (BuildRuntime, ReportRuntime) {{ unreachable!() }}\n"
    );
    assert_rejected(validate(&lifecycle, &custom, &tuple_getter), "all_views");

    let tuple_struct = format!("{hosting}\nstruct TupleBroker((BuildRuntime, ReportRuntime));\n");
    assert_rejected(validate(&lifecycle, &custom, &tuple_struct), "TupleBroker");

    let tuple_field =
        format!("{hosting}\nstruct TupleFieldBroker {{ views: (BuildRuntime, ReportRuntime) }}\n");
    assert_rejected(
        validate(&lifecycle, &custom, &tuple_field),
        "TupleFieldBroker",
    );

    let local_broker = format!(
        "{hosting}\nfn leaked_local() {{ struct LocalBroker {{ build: BuildRuntime, report: ReportRuntime }} }}\n"
    );
    assert_rejected(validate(&lifecycle, &custom, &local_broker), "LocalBroker");

    let report = source("src/host/report.rs");
    let leaked_report =
        format!("{report}\nfn leaked_consumer(runtime: BuildRuntime) {{ let _ = runtime; }}\n");
    let leaked_report = syn::parse_file(&leaked_report).unwrap();
    let report_sites = capability_signature_sites(&leaked_report);
    let expected_report_sites = names(&["AgentReporter::report"]);
    let result = if report_sites == expected_report_sites {
        Ok(())
    } else {
        Err(format!(
            "report capability consumer inventory changed: {report_sites:#?}"
        ))
    };
    assert_rejected(result, "fn leaked_consumer");

    let nested_signature_consumer = hosting.replace(
        "    fn abort(&self) {\n",
        "    fn abort(&self) {\n        fn nested_view_consumer(runtime: BuildRuntime) { let _ = runtime; }\n",
    );
    assert_rejected(
        validate(&lifecycle, &custom, &nested_signature_consumer),
        "fn nested_view_consumer",
    );

    let universal_effect_facade = format!(
        "{hosting}\nstruct UniversalEffectFacade {{ host: Arc<RuntimeHost> }}\nimpl UniversalEffectFacade {{ async fn execute(&self, effect: RuntimeEffect) -> Result<RuntimeEffectExecution> {{ self.host.prepare_effect(effect).await }} }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &universal_effect_facade),
        "UniversalEffectFacade",
    );

    let aliased_broker = format!(
        "{hosting}\nuse self::BuildRuntime as B;\nuse self::ReportRuntime as R;\nuse self::OutboundRuntime as O;\nstruct RenamedBroker {{ build: B, report: R, outbound: O }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &aliased_broker),
        "guarded import alias",
    );

    let broad_report = format!(
        "{report}\nasync fn additional_report_worker(runtime: &crate::host::hosting::PodRuntime) {{ runtime.abort(); }}\n"
    );
    let broad_report = syn::parse_file(&broad_report).unwrap();
    let report_sites = capability_signature_sites(&broad_report);
    let result = if report_sites == expected_report_sites {
        Ok(())
    } else {
        Err(format!(
            "report capability consumer inventory changed: {report_sites:#?}"
        ))
    };
    assert_rejected(result, "fn additional_report_worker");

    let report_broad_field = format!(
        "{report}\nstruct UniversalReportFacade {{ runtime: std::sync::Arc<crate::host::hosting::PodRuntime> }}\n"
    );
    let report_broad_field = syn::parse_file(&report_broad_field).unwrap();
    assert_rejected(
        reject_unclassified_capability_aggregates(&report_broad_field, &[]),
        "UniversalReportFacade",
    );

    let type_alias_broker = format!(
        "{hosting}\ntype B = BuildRuntime;\ntype R = ReportRuntime;\ntype O = OutboundRuntime;\nstruct TypeAliasBroker {{ build: B, report: R, outbound: O }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &type_alias_broker),
        "capability alias",
    );

    let scoped_alias_broker = format!(
        "{hosting}\nuse self::BuildRuntime as B;\nuse self::ReportRuntime as R;\nuse self::OutboundRuntime as O;\nstruct ScopeBroker {{ build: B, report: R, outbound: O }}\nmod unrelated {{ use std::string::String as B; use std::string::String as R; use std::string::String as O; }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &scoped_alias_broker),
        "guarded import alias",
    );

    let transport = source("src/host/transport.rs");
    let broadened_outbound = transport.replace(
        "    runtime: Arc<OutboundRuntime>,\n    transport: Arc<Mutex<ReliableTransport>>,",
        "    runtime: Arc<OutboundRuntime>,\n    unrelated_report: crate::host::hosting::ReportRuntime,\n    transport: Arc<Mutex<ReliableTransport>>,",
    );
    let broadened_outbound = syn::parse_file(&broadened_outbound).unwrap();
    assert_rejected(
        reject_aggregate_signatures(&broadened_outbound),
        "run_outbound",
    );

    let reversed_scope_aliases = format!(
        "{hosting}\nmod unrelated_first {{ use std::string::String as B; use std::string::String as R; use std::string::String as O; }}\nuse self::BuildRuntime as B;\nuse self::ReportRuntime as R;\nuse self::OutboundRuntime as O;\nstruct ReversedScopeBroker {{ build: B, report: R, outbound: O }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &reversed_scope_aliases),
        "guarded import alias",
    );

    let wrapped_aliases = format!(
        "{hosting}\ntype B = Arc<BuildRuntime>;\ntype R = Arc<ReportRuntime>;\ntype O = Arc<OutboundRuntime>;\nstruct WrappedAliasBroker {{ build: B, report: R, outbound: O }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &wrapped_aliases),
        "capability alias",
    );

    let forward_aliases = format!(
        "{hosting}\ntype B = LaterB;\ntype R = LaterR;\ntype O = LaterO;\ntype LaterB = BuildRuntime;\ntype LaterR = ReportRuntime;\ntype LaterO = OutboundRuntime;\nstruct ForwardAliasBroker {{ build: B, report: R, outbound: O }}\n"
    );
    assert_rejected(
        validate(&lifecycle, &custom, &forward_aliases),
        "capability alias",
    );

    let aliased_outbound = format!(
        "type ExtraReport = std::sync::Arc<crate::host::hosting::ReportRuntime>;\n{transport}"
    )
    .replace(
        "    runtime: Arc<OutboundRuntime>,\n    transport: Arc<Mutex<ReliableTransport>>,",
        "    runtime: Arc<OutboundRuntime>,\n    unrelated_report: ExtraReport,\n    transport: Arc<Mutex<ReliableTransport>>,",
    );
    let aliased_outbound = syn::parse_file(&aliased_outbound).unwrap();
    assert_rejected(reject_broad_aliases(&aliased_outbound), "ExtraReport");

    let module_root = tempfile::tempdir().unwrap();
    let workers = module_root.path().join("workers");
    fs::create_dir(&workers).unwrap();
    fs::write(
        workers.join("hosting.rs"),
        "struct UncheckedOutOfLineFacade { runtime: std::sync::Arc<crate::host::hosting::PodRuntime> }",
    )
    .unwrap();
    let path = production_rust_files(module_root.path())
        .into_iter()
        .next()
        .unwrap();
    let relative = path.strip_prefix(module_root.path()).unwrap();
    let source = fs::read_to_string(&path).unwrap();
    assert_rejected(
        validate_production_module(relative, &source).map(|_| ()),
        "workers/hosting.rs",
    );
}
