use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use quote::ToTokens;
use syn::punctuated::Punctuated;
use syn::visit::Visit;
use syn::{Expr, ImplItem, Item, Lit, Meta, Token, TraitItem, Type};

#[rustfmt::skip]
const CAPABILITY_TRAITS: &[&str] = &[
    "ProcessLifecycle", "AuthorityLifecycle", "AccessLifecycle", "BuildLifecycle",
    "BuildCancellation", "TopologyLifecycle", "LifecycleObservation", "OutboundLifecycle",
];
#[rustfmt::skip]
const HOST_CAPABILITY_TRAITS: &[&str] =
    &["ReportHost", "BuildHost", "BuildAttemptHost", "PeerDiscoveryHost", "OutboundHost"];
#[rustfmt::skip]
const CAPABILITY_TYPES: &[&str] = &[
    "ProcessRuntime", "AuthorityRuntime", "PeerRuntime", "AccessClosure", "AccessRuntime",
    "ReportObservationRuntime", "EvidenceRuntime", "EffectEvidenceRuntime", "BuildLifecycleRuntime",
    "BuildCancellationRuntime", "OutboundLifecycleRuntime", "RemovalWitnessRuntime",
    "TopologyRuntime", "RecoveryRuntime", "ReportRuntime", "RecoveryOwnerRuntime", "BuildRuntime",
    "BuildAttemptRuntime", "PeerDiscoveryRuntime", "OutboundRuntime",
];
#[rustfmt::skip]
const BROAD_OWNER_TYPES: &[&str] = &[
    "RuntimeHost", "PodRuntime", "RegisteredReplicator", "LifecycleWiring",
    "ReplicatorLifecycleRegistration", "ManagedLifecycleBackend", "CustomReplicatorHost",
];
#[rustfmt::skip]
const LIFECYCLE_VIEW_RULES: &[(&str, &[&str])] = &[
    ("ProcessRuntime", &["dynProcessLifecycle"]),
    ("AuthorityRuntime", &["dynAuthorityLifecycle"]),
    ("PeerRuntime", &["dynAuthorityLifecycle"]),
    ("AccessClosure", &["dynAccessLifecycle"]),
    ("AccessRuntime", &["dynAccessLifecycle"]),
    ("ReportObservationRuntime", &["dynLifecycleObservation"]),
    ("EvidenceRuntime", &["dynLifecycleObservation"]),
    ("EffectEvidenceRuntime", &["dynLifecycleObservation"]),
    ("BuildLifecycleRuntime", &["dynBuildLifecycle", "dynBuildCancellation", "BuildCancellationRuntime"]),
    ("BuildCancellationRuntime", &["dynBuildCancellation"]),
    ("OutboundLifecycleRuntime", &["dynOutboundLifecycle"]),
    ("RemovalWitnessRuntime", &["dynTopologyLifecycle"]),
    ("TopologyRuntime", &["dynTopologyLifecycle"]),
    ("RecoveryRuntime", &["dynAuthorityLifecycle", "dynAccessLifecycle", "dynTopologyLifecycle", "dynLifecycleObservation"]),
];
#[rustfmt::skip]
const HOSTING_VIEW_RULES: &[(&str, &[&str])] = &[
    ("ReportRuntime", &["dynReportHost"]),
    ("RecoveryOwnerRuntime", &["dynRecoveryOwnerHost"]),
    ("BuildRuntime", &["dynBuildHost", "BuildAttemptRuntime"]),
    ("BuildAttemptRuntime", &["dynBuildAttemptHost"]),
    ("PeerDiscoveryRuntime", &["dynPeerDiscoveryHost"]),
    ("OutboundRuntime", &["dynOutboundHost"]),
];

fn source(path: &str) -> String {
    fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap()
        .replace("\r\n", "\n")
}

fn compact<T: ToTokens>(value: &T) -> String {
    value.to_token_stream().to_string().replace(' ', "")
}

#[derive(Default)]
struct GuardedMarkers {
    capabilities: BTreeSet<String>,
    broad: BTreeSet<String>,
    direct_primary: bool,
}

impl<'ast> Visit<'ast> for GuardedMarkers {
    fn visit_path(&mut self, path: &'ast syn::Path) {
        for segment in &path.segments {
            let name = segment.ident.to_string();
            if CAPABILITY_TYPES.contains(&name.as_str()) {
                self.capabilities.insert(name.clone());
            } else if CAPABILITY_TRAITS.contains(&name.as_str())
                || HOST_CAPABILITY_TRAITS.contains(&name.as_str())
            {
                self.capabilities.insert(format!("dyn{name}"));
            }
            if BROAD_OWNER_TYPES.contains(&name.as_str()) {
                self.broad.insert(name);
            }
            if segment.ident == "PrimaryReplicator" {
                self.direct_primary = true;
            }
        }
        syn::visit::visit_path(self, path);
    }
}

fn type_markers(ty: &Type) -> GuardedMarkers {
    let mut markers = GuardedMarkers::default();
    markers.visit_type(ty);
    markers
}

fn signature_markers(signature: &syn::Signature) -> GuardedMarkers {
    let mut markers = GuardedMarkers::default();
    markers.visit_signature(signature);
    markers
}

fn bound_markers(bounds: &Punctuated<syn::TypeParamBound, Token![+]>) -> GuardedMarkers {
    let mut markers = GuardedMarkers::default();
    for bound in bounds {
        markers.visit_type_param_bound(bound);
    }
    markers
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

fn impl_name(item: &syn::ItemImpl) -> String {
    type_name(&item.self_ty).unwrap_or_else(|| "<unknown>".into())
}

fn module_path(source_path: &Path, item: &syn::ItemMod) -> Option<PathBuf> {
    for attribute in &item.attrs {
        if attribute.path().is_ident("path")
            && let Meta::NameValue(value) = &attribute.meta
            && let Expr::Lit(value) = &value.value
            && let Lit::Str(value) = &value.lit
        {
            return Some(source_path.parent().unwrap().join(value.value()));
        }
    }
    let parent = source_path.parent().unwrap();
    let base = match source_path.file_name().and_then(|name| name.to_str()) {
        Some("mod.rs" | "lib.rs" | "main.rs") => parent.to_path_buf(),
        _ => parent.join(source_path.file_stem().unwrap()),
    };
    let direct = base.join(format!("{}.rs", item.ident));
    if direct.is_file() {
        Some(direct)
    } else {
        let nested = base.join(item.ident.to_string()).join("mod.rs");
        nested.is_file().then_some(nested)
    }
}

fn disallowed_type_lint_exempt(attributes: &[syn::Attribute], inherited: bool) -> bool {
    let mentions_lint =
        |attribute: &syn::Attribute| compact(&attribute.meta).contains("clippy::disallowed_types");
    let mut exempt = inherited;
    for attribute in attributes
        .iter()
        .filter(|attribute| mentions_lint(attribute))
    {
        if attribute.path().is_ident("allow") {
            exempt = true;
        } else if attribute.path().is_ident("deny") || attribute.path().is_ident("forbid") {
            exempt = false;
        }
    }
    exempt
}

fn production_modules(root: &Path) -> Vec<(PathBuf, bool)> {
    fn collect(
        root: &Path,
        path: &Path,
        inherited: bool,
        modules: &mut Vec<(PathBuf, bool)>,
        visited: &mut BTreeSet<PathBuf>,
    ) {
        let path = path.to_path_buf();
        if !visited.insert(path.clone()) {
            return;
        }
        let source = fs::read_to_string(&path).unwrap();
        let file = syn::parse_file(&source).unwrap();
        let effective = disallowed_type_lint_exempt(&file.attrs, inherited);
        modules.push((path.strip_prefix(root).unwrap().to_path_buf(), effective));
        for item in &file.items {
            let Item::Mod(item) = item else {
                continue;
            };
            if excluded_from_production(&item.attrs) {
                continue;
            }
            let child_exempt = disallowed_type_lint_exempt(&item.attrs, effective);
            if item.content.is_none()
                && let Some(child) = module_path(&path, item)
            {
                collect(root, &child, child_exempt, modules, visited);
            }
        }
    }

    let mut modules = Vec::new();
    let mut visited = BTreeSet::new();
    collect(
        root,
        &root.join("mod.rs"),
        false,
        &mut modules,
        &mut visited,
    );
    fn discover(root: &Path, path: &Path, modules: &mut Vec<(PathBuf, bool)>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().and_then(|name| name.to_str()) != Some("tests") {
                    discover(root, &path, modules);
                }
            } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                if !modules.iter().any(|(known, _)| known == &relative) {
                    modules.push((relative, true));
                }
            }
        }
    }
    discover(root, root, &mut modules);
    modules.sort();
    modules
}

fn allowed_aggregates(relative: &Path) -> &'static [&'static str] {
    match relative.to_str() {
        Some("lifecycle.rs") => &[
            "LifecycleWiring",
            "ReportObservationRuntime",
            "BuildLifecycleRuntime",
            "RecoveryRuntime",
        ],
        Some("custom.rs") => &[
            "ReplicatorLifecycleRegistration",
            "AcceptedAccessEffect",
            "ManagedLifecycleBackend",
            "CustomReplicatorHost",
        ],
        Some("custom/authority.rs") => &["CustomAuthorityContainment", "CustomAuthorityAttempt"],
        Some("hosting.rs") => &[
            "RegisteredReplicator",
            "HostedPrimaryReplicator",
            "BuildRuntime",
            "PodRuntime",
            "RuntimeDataPlane",
            "RuntimeHost",
            "RecoveryWake",
            "HostAccessView",
            "OpenAttempt",
        ],
        Some("process.rs") => &["ReplicaHandle"],
        Some("service.rs") => &["AgentService"],
        Some("testing.rs") => &["Endpoint"],
        _ => &[],
    }
}

fn view_rule(name: &str) -> Option<&'static [&'static str]> {
    LIFECYCLE_VIEW_RULES
        .iter()
        .chain(HOSTING_VIEW_RULES)
        .find_map(|(view, allowed)| (*view == name).then_some(*allowed))
}

fn reject_transitive_capability_inheritance(file: &syn::File) -> Result<(), String> {
    let graph = file
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Trait(item) => Some((
                item.ident.to_string(),
                item.supertraits
                    .iter()
                    .filter_map(|bound| match bound {
                        syn::TypeParamBound::Trait(bound) => bound
                            .path
                            .segments
                            .last()
                            .map(|value| value.ident.to_string()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    fn reachable_capabilities(
        name: &str,
        graph: &BTreeMap<String, Vec<String>>,
        visited: &mut BTreeSet<String>,
    ) -> BTreeSet<String> {
        let mut capabilities = BTreeSet::new();
        for parent in graph.get(name).into_iter().flatten() {
            if CAPABILITY_TRAITS.contains(&parent.as_str())
                || HOST_CAPABILITY_TRAITS.contains(&parent.as_str())
            {
                capabilities.insert(parent.clone());
            } else if visited.insert(parent.clone()) {
                capabilities.extend(reachable_capabilities(parent, graph, visited));
            }
        }
        capabilities
    }
    for name in graph.keys() {
        let capabilities = reachable_capabilities(name, &graph, &mut BTreeSet::new());
        let guarded = CAPABILITY_TRAITS.contains(&name.as_str())
            || HOST_CAPABILITY_TRAITS.contains(&name.as_str());
        if (guarded && !capabilities.is_empty()) || capabilities.len() >= 2 {
            return Err(format!(
                "{name} transitively aggregates lifecycle capabilities"
            ));
        }
    }
    for item in &file.items {
        if let Item::Mod(item) = item
            && let Some((_, items)) = &item.content
        {
            reject_transitive_capability_inheritance(&syn::File {
                frontmatter: None,
                shebang: None,
                attrs: Vec::new(),
                items: items.clone(),
            })?;
        }
    }
    Ok(())
}

fn validate_module_policy(
    file: &syn::File,
    allowed: &[&str],
    reject_broad: bool,
) -> Result<(), String> {
    reject_transitive_capability_inheritance(file)?;
    struct PolicyVisitor<'a> {
        allowed: &'a [&'a str],
        reject_broad: bool,
        depth: usize,
        issue: Option<String>,
    }

    impl PolicyVisitor<'_> {
        fn record(&mut self, issue: impl FnOnce() -> String) {
            if self.issue.is_none() {
                self.issue = Some(issue());
            }
        }

        fn signature(&mut self, owner: &str, signature: &syn::Signature) {
            if owner != "LifecycleWiring::new" {
                let markers = signature_markers(signature);
                if markers.capabilities.len() + markers.broad.len() >= 2 {
                    self.record(|| format!("{owner} exposes an aggregate capability signature"));
                }
            }
        }
    }

    fn inspect_use(tree: &syn::UseTree, visitor: &mut PolicyVisitor<'_>) {
        match tree {
            syn::UseTree::Path(path) => inspect_use(&path.tree, visitor),
            syn::UseTree::Rename(rename) => {
                let target = rename.ident.to_string();
                if CAPABILITY_TYPES.contains(&target.as_str())
                    || CAPABILITY_TRAITS.contains(&target.as_str())
                    || HOST_CAPABILITY_TRAITS.contains(&target.as_str())
                    || target == "PrimaryReplicator"
                    || (visitor.reject_broad && BROAD_OWNER_TYPES.contains(&target.as_str()))
                {
                    visitor
                        .record(|| format!("guarded import alias {} -> {target}", rename.rename));
                }
            }
            syn::UseTree::Group(group) => {
                for tree in &group.items {
                    inspect_use(tree, visitor);
                }
            }
            _ => {}
        }
    }

    impl<'ast> Visit<'ast> for PolicyVisitor<'_> {
        fn visit_item_use(&mut self, item: &'ast syn::ItemUse) {
            inspect_use(&item.tree, self);
            syn::visit::visit_item_use(self, item);
        }

        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            let markers = type_markers(&item.ty);
            if !markers.capabilities.is_empty()
                || markers.direct_primary
                || (self.reject_broad && !markers.broad.is_empty())
            {
                let target = compact(&item.ty);
                self.record(|| format!("guarded type alias {} -> {target}", item.ident));
            }
            syn::visit::visit_item_type(self, item);
        }

        fn visit_item_struct(&mut self, item: &'ast syn::ItemStruct) {
            let capabilities = item
                .fields
                .iter()
                .flat_map(|field| type_markers(&field.ty).capabilities)
                .collect::<BTreeSet<_>>();
            let broad = item
                .fields
                .iter()
                .flat_map(|field| type_markers(&field.ty).broad)
                .collect::<BTreeSet<_>>();
            let approved =
                self.depth == 0 && self.allowed.contains(&item.ident.to_string().as_str());
            let reject_broad = disallowed_type_lint_exempt(&item.attrs, self.reject_broad);
            if capabilities.len() >= 2 && !approved {
                self.record(|| format!("{} is an unapproved capability aggregate", item.ident));
            } else if reject_broad && !broad.is_empty() && !approved {
                self.record(|| format!("{} retains an unapproved broad owner", item.ident));
            }
            syn::visit::visit_item_struct(self, item);
        }

        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            self.signature(&format!("fn {}", item.sig.ident), &item.sig);
            syn::visit::visit_item_fn(self, item);
        }

        fn visit_item_trait(&mut self, item: &'ast syn::ItemTrait) {
            let name = item.ident.to_string();
            let guarded = CAPABILITY_TRAITS.contains(&name.as_str())
                || HOST_CAPABILITY_TRAITS.contains(&name.as_str());
            let supertraits = bound_markers(&item.supertraits).capabilities;
            if guarded && !supertraits.is_empty() {
                self.record(|| format!("{} inherits an unrelated capability trait", item.ident));
            } else if !guarded && supertraits.len() >= 2 {
                self.record(|| {
                    format!(
                        "{} combines multiple lifecycle capability traits",
                        item.ident
                    )
                });
            }
            for member in &item.items {
                if let TraitItem::Fn(method) = member {
                    let owner = format!("{}::{}", item.ident, method.sig.ident);
                    let markers = signature_markers(&method.sig);
                    if guarded
                        && (!markers.capabilities.is_empty()
                            || !markers.broad.is_empty()
                            || markers.direct_primary)
                    {
                        self.record(|| format!("{owner} exposes another lifecycle capability"));
                    } else {
                        self.signature(&owner, &method.sig);
                    }
                }
            }
            syn::visit::visit_item_trait(self, item);
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            let owner = impl_name(item);
            if let Some((path, _)) = &item.trait_ {
                let mut markers = type_markers(&item.self_ty);
                markers.visit_path(path);
                let registration = markers.broad.contains("ReplicatorLifecycleRegistration")
                    || markers.broad.contains("RegisteredReplicator");
                let complete = [
                    "LifecycleWiring",
                    "ManagedLifecycleBackend",
                    "CustomReplicatorHost",
                ]
                .iter()
                .any(|name| markers.broad.contains(*name));
                if registration && complete {
                    self.record(|| "conversion exposes complete lifecycle wiring".into());
                }
            }
            let registration = matches!(
                owner.as_str(),
                "RegisteredReplicator" | "ReplicatorLifecycleRegistration"
            );
            if registration && item.trait_.is_some() {
                self.record(|| format!("{owner} implements an unapproved wiring conversion"));
            }
            if view_rule(&owner).is_some() && item.trait_.is_some() {
                self.record(|| format!("{owner} implements an unapproved conversion trait"));
            }
            for member in &item.items {
                if let ImplItem::Fn(method) = member {
                    let method_owner = format!("{owner}::{}", method.sig.ident);
                    let markers = signature_markers(&method.sig);
                    if registration {
                        let output = return_markers(&method.sig.output);
                        if output.broad.iter().any(|name| {
                            [
                                "LifecycleWiring",
                                "ManagedLifecycleBackend",
                                "CustomReplicatorHost",
                            ]
                            .contains(&name.as_str())
                        }) {
                            self.record(|| {
                                format!("{method_owner} exposes complete lifecycle wiring")
                            });
                        }
                    }
                    if let Some(allowed) = view_rule(&owner)
                        && (markers.direct_primary
                            || !markers.broad.is_empty()
                            || markers
                                .capabilities
                                .iter()
                                .any(|value| !allowed.contains(&value.as_str())))
                    {
                        self.record(|| format!("{method_owner} exposes an unrelated capability"));
                    }
                    self.signature(&method_owner, &method.sig);
                }
            }
            syn::visit::visit_item_impl(self, item);
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            let parent = self.reject_broad;
            self.reject_broad = disallowed_type_lint_exempt(&item.attrs, parent);
            self.depth += 1;
            syn::visit::visit_item_mod(self, item);
            self.depth -= 1;
            self.reject_broad = parent;
        }

        fn visit_block(&mut self, block: &'ast syn::Block) {
            self.depth += 1;
            syn::visit::visit_block(self, block);
            self.depth -= 1;
        }
    }

    let mut visitor = PolicyVisitor {
        allowed,
        reject_broad,
        depth: 0,
        issue: None,
    };
    visitor.visit_file(file);
    visitor.issue.map_or(Ok(()), Err)
}

fn validate_view_boundaries(file: &syn::File, rules: &[(&str, &[&str])]) -> Result<(), String> {
    for (view, allowed) in rules {
        let item = file
            .items
            .iter()
            .find_map(|item| match item {
                Item::Struct(item) if item.ident == *view => Some(item),
                _ => None,
            })
            .ok_or_else(|| format!("{view} missing"))?;
        for field in &item.fields {
            let markers = type_markers(&field.ty);
            if !markers.broad.is_empty() || markers.direct_primary {
                return Err(format!("{view} retains a broad owner"));
            }
            if markers
                .capabilities
                .iter()
                .any(|capability| !allowed.contains(&capability.as_str()))
            {
                return Err(format!("{view} retains an unrelated capability"));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProductionCfg {
    Enabled,
    Disabled,
    Unknown,
}

fn cfg_in_production(meta: &Meta) -> ProductionCfg {
    match meta {
        Meta::Path(path) if path.is_ident("test") => ProductionCfg::Disabled,
        Meta::Path(_) => ProductionCfg::Unknown,
        Meta::NameValue(value) if value.path.is_ident("feature") => {
            if matches!(
                &value.value,
                Expr::Lit(value) if matches!(&value.lit, Lit::Str(value) if value.value() == "testing")
            ) {
                ProductionCfg::Disabled
            } else {
                ProductionCfg::Unknown
            }
        }
        Meta::NameValue(_) => ProductionCfg::Unknown,
        Meta::List(list) => {
            let Ok(arguments) =
                list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
            else {
                return ProductionCfg::Unknown;
            };
            if list.path.is_ident("all") {
                if arguments
                    .iter()
                    .any(|argument| cfg_in_production(argument) == ProductionCfg::Disabled)
                {
                    ProductionCfg::Disabled
                } else if arguments
                    .iter()
                    .all(|argument| cfg_in_production(argument) == ProductionCfg::Enabled)
                {
                    ProductionCfg::Enabled
                } else {
                    ProductionCfg::Unknown
                }
            } else if list.path.is_ident("any") {
                if arguments
                    .iter()
                    .any(|argument| cfg_in_production(argument) == ProductionCfg::Enabled)
                {
                    ProductionCfg::Enabled
                } else if arguments
                    .iter()
                    .all(|argument| cfg_in_production(argument) == ProductionCfg::Disabled)
                {
                    ProductionCfg::Disabled
                } else {
                    ProductionCfg::Unknown
                }
            } else if list.path.is_ident("not") && arguments.len() == 1 {
                match cfg_in_production(arguments.first().unwrap()) {
                    ProductionCfg::Enabled => ProductionCfg::Disabled,
                    ProductionCfg::Disabled => ProductionCfg::Enabled,
                    ProductionCfg::Unknown => ProductionCfg::Unknown,
                }
            } else {
                ProductionCfg::Unknown
            }
        }
    }
}

fn excluded_from_production(attributes: &[syn::Attribute]) -> bool {
    attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("cfg"))
        .filter_map(|attribute| attribute.parse_args::<Meta>().ok())
        .any(|meta| cfg_in_production(&meta) == ProductionCfg::Disabled)
}

macro_rules! expression_attributes {
    ($expression:expr; $($variant:ident),+ $(,)?) => {
        match $expression {
            $(Expr::$variant(value) => &value.attrs,)+
            _ => &[],
        }
    };
}

#[rustfmt::skip]
fn expression_attributes(expression: &Expr) -> &[syn::Attribute] {
    expression_attributes!(expression;
        Array, Assign, Async, Await, Binary, Block, Break, Call, Cast, Closure,
        Const, Continue, Field, ForLoop, Group, If, Index, Infer, Let, Lit, Loop,
        Macro, Match, MethodCall, Paren, Path, Range, RawAddr, Reference, Repeat,
        Return, Struct, Try, TryBlock, Tuple, Unary, Unsafe, While, Yield,
    )
}

fn method_attributes<'a>(
    file: &'a syn::File,
    owner: &str,
    method_name: &str,
    is_trait: bool,
) -> Option<&'a [syn::Attribute]> {
    file.items.iter().find_map(|item| match item {
        Item::Trait(item) if is_trait && item.ident == owner => {
            item.items.iter().find_map(|member| match member {
                TraitItem::Fn(method) if method.sig.ident == method_name => {
                    Some(method.attrs.as_slice())
                }
                _ => None,
            })
        }
        Item::Impl(item) if !is_trait && type_name(&item.self_ty).as_deref() == Some(owner) => {
            item.items.iter().find_map(|member| match member {
                ImplItem::Fn(method) if method.sig.ident == method_name => {
                    Some(method.attrs.as_slice())
                }
                _ => None,
            })
        }
        _ => None,
    })
}

fn validate_fixture_gates(file: &syn::File) -> Result<(), String> {
    for (owner, methods, is_trait) in [
        ("ProcessLifecycle", &["invalidate_public_access"][..], true),
        (
            "BuildLifecycle",
            &["build_replica", "remove_replica"][..],
            true,
        ),
        ("ProcessRuntime", &["invalidate_public_access"][..], false),
        (
            "BuildLifecycleRuntime",
            &["build_replica", "remove_replica"][..],
            false,
        ),
    ] {
        for method_name in methods {
            let attributes = method_attributes(file, owner, method_name, is_trait)
                .ok_or_else(|| format!("{owner}::{method_name} missing"))?;
            if !excluded_from_production(attributes) {
                return Err(format!(
                    "{owner}::{method_name} is not excluded from production"
                ));
            }
        }
    }
    Ok(())
}

fn return_markers(output: &syn::ReturnType) -> GuardedMarkers {
    let mut markers = GuardedMarkers::default();
    markers.visit_return_type(output);
    markers
}

fn block_markers(block: &syn::Block) -> GuardedMarkers {
    let mut markers = GuardedMarkers::default();
    markers.visit_block(block);
    markers
}

fn reject_wiring_escape(file: &syn::File, owner: &str, forbidden: &[&str]) -> Result<(), String> {
    let item = file
        .items
        .iter()
        .find_map(|item| match item {
            Item::Struct(item) if item.ident == owner => Some(item),
            _ => None,
        })
        .ok_or_else(|| format!("{owner} missing"))?;
    for field in &item.fields {
        let markers = type_markers(&field.ty);
        if markers
            .broad
            .iter()
            .any(|marker| forbidden.contains(&marker.as_str()))
        {
            return Err(format!("{owner} retains complete lifecycle wiring"));
        }
    }
    Ok(())
}

fn validate_cancellation_owner(file: &syn::File, owner: &str) -> Result<(), String> {
    let method = file
        .items
        .iter()
        .find_map(|item| match item {
            Item::Impl(item) if type_name(&item.self_ty).as_deref() == Some(owner) => {
                item.items.iter().find_map(|member| match member {
                    ImplItem::Fn(method) if method.sig.ident == "new" => Some(method),
                    _ => None,
                })
            }
            _ => None,
        })
        .ok_or_else(|| format!("{owner}::new missing"))?;
    let signature = signature_markers(&method.sig);
    let expected = BTreeSet::from(["BuildAttemptRuntime".to_owned()]);
    if signature.capabilities != expected || !signature.broad.is_empty() || signature.direct_primary
    {
        return Err(format!(
            "{owner}::new does not retain cancellation-only ownership"
        ));
    }
    let body = block_markers(&method.block);
    if !body.broad.is_empty()
        || body.direct_primary
        || body
            .capabilities
            .iter()
            .any(|capability| capability != "BuildAttemptRuntime")
    {
        return Err(format!("{owner}::new captures a broader runtime owner"));
    }
    Ok(())
}

fn validate_transport_routing(source: &str) -> Result<(), String> {
    fn called_path(expression: &Expr) -> Option<&syn::Path> {
        match expression {
            Expr::Path(path) => Some(&path.path),
            Expr::Paren(value) => called_path(&value.expr),
            Expr::Group(value) => called_path(&value.expr),
            _ => None,
        }
    }

    struct CallVisitor {
        admitted: usize,
        direct: usize,
    }

    impl CallVisitor {
        fn observe(&mut self, name: &str) {
            match name {
                "execute_admitted_build" => self.admitted += 1,
                "build_replica" => self.direct += 1,
                _ => {}
            }
        }
    }

    impl<'ast> Visit<'ast> for CallVisitor {
        fn visit_item_fn(&mut self, item: &'ast syn::ItemFn) {
            if !excluded_from_production(&item.attrs) {
                syn::visit::visit_item_fn(self, item);
            }
        }

        fn visit_item_impl(&mut self, item: &'ast syn::ItemImpl) {
            if !excluded_from_production(&item.attrs) {
                syn::visit::visit_item_impl(self, item);
            }
        }

        fn visit_impl_item_fn(&mut self, item: &'ast syn::ImplItemFn) {
            if !excluded_from_production(&item.attrs) {
                syn::visit::visit_impl_item_fn(self, item);
            }
        }

        fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
            if !excluded_from_production(&item.attrs) {
                syn::visit::visit_item_mod(self, item);
            }
        }

        fn visit_local(&mut self, item: &'ast syn::Local) {
            if !excluded_from_production(&item.attrs) {
                syn::visit::visit_local(self, item);
            }
        }

        fn visit_arm(&mut self, item: &'ast syn::Arm) {
            if !excluded_from_production(&item.attrs) {
                syn::visit::visit_arm(self, item);
            }
        }

        fn visit_stmt_macro(&mut self, item: &'ast syn::StmtMacro) {
            if !excluded_from_production(&item.attrs) {
                syn::visit::visit_stmt_macro(self, item);
            }
        }

        fn visit_expr(&mut self, expression: &'ast Expr) {
            if !excluded_from_production(expression_attributes(expression)) {
                syn::visit::visit_expr(self, expression);
            }
        }

        fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
            self.observe(&call.method.to_string());
            syn::visit::visit_expr_method_call(self, call);
        }

        fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
            if let Some(path) = called_path(&call.func)
                && let Some(segment) = path.segments.last()
            {
                self.observe(&segment.ident.to_string());
            }
            syn::visit::visit_expr_call(self, call);
        }
    }

    let file = syn::parse_file(source).map_err(|error| format!("parse transport: {error}"))?;
    let mut visitor = CallVisitor {
        admitted: 0,
        direct: 0,
    };
    visitor.visit_file(&file);
    if visitor.admitted != 1 {
        return Err(format!(
            "transport has {} admitted-build coordinator call sites",
            visitor.admitted
        ));
    }
    if visitor.direct != 0 {
        return Err("transport contains a direct primary build call".into());
    }
    Ok(())
}

fn validate_production_module(
    relative: &Path,
    source: &str,
    reject_broad: bool,
) -> Result<(), String> {
    let display = relative.to_string_lossy();
    for forbidden in ["ReplicatorLifecycleBackend", "ReplicatorLifecycleHost"] {
        if source.contains(forbidden) {
            return Err(format!("{display}: universal lifecycle facade {forbidden}"));
        }
    }
    let file = syn::parse_file(source).map_err(|error| format!("{display}: {error}"))?;
    validate_module_policy(&file, allowed_aggregates(relative), reject_broad)
        .map_err(|error| format!("{display}: {error}"))?;
    Ok(())
}

fn validate_project() -> Result<(), String> {
    let host_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/host");
    for (relative, lint_exempt) in production_modules(&host_root) {
        let source = fs::read_to_string(host_root.join(&relative)).unwrap();
        validate_production_module(&relative, &source, lint_exempt)?;
    }

    let lifecycle = source("src/host/lifecycle.rs");
    let lifecycle_file =
        syn::parse_file(&lifecycle).map_err(|error| format!("parse lifecycle: {error}"))?;
    validate_view_boundaries(&lifecycle_file, LIFECYCLE_VIEW_RULES)?;
    validate_fixture_gates(&lifecycle_file)?;

    let custom = source("src/host/custom.rs");
    let custom_file = syn::parse_file(&custom).map_err(|error| format!("parse custom: {error}"))?;
    reject_wiring_escape(
        &custom_file,
        "ReplicatorLifecycleRegistration",
        &[
            "LifecycleWiring",
            "ManagedLifecycleBackend",
            "CustomReplicatorHost",
        ],
    )?;

    let hosting = source("src/host/hosting.rs");
    let hosting_file =
        syn::parse_file(&hosting).map_err(|error| format!("parse hosting: {error}"))?;
    validate_view_boundaries(&hosting_file, HOSTING_VIEW_RULES)?;
    reject_wiring_escape(
        &hosting_file,
        "RegisteredReplicator",
        &[
            "LifecycleWiring",
            "ReplicatorLifecycleRegistration",
            "ManagedLifecycleBackend",
            "CustomReplicatorHost",
        ],
    )?;
    validate_cancellation_owner(&hosting_file, "BuildRuntimeCancellation")?;

    let transport = source("src/host/transport.rs");
    let transport_file =
        syn::parse_file(&transport).map_err(|error| format!("parse transport: {error}"))?;
    validate_cancellation_owner(&transport_file, "BuildDispatchCancellation")?;
    validate_transport_routing(&transport)?;
    Ok(())
}

fn assert_rejected(result: Result<(), String>, expected: &str) {
    let error = result.expect_err("mutation unexpectedly passed validation");
    assert!(
        error.contains(expected),
        "diagnostic did not identify {expected:?}: {error}"
    );
}

fn parsed(source: &str) -> syn::File {
    syn::parse_file(source).unwrap()
}

fn reject_policy(source: &str, broad: bool, expected: &str) {
    assert_rejected(
        validate_module_policy(&parsed(source), &[], broad),
        expected,
    );
}

fn reject_transport(source: &str, expected: &str) {
    assert_rejected(validate_transport_routing(source), expected);
}

fn reject_cancellation(source: &str, expected: &str) {
    assert_rejected(
        validate_cancellation_owner(&parsed(source), "BuildRuntimeCancellation"),
        expected,
    );
}

fn production_items(source: &str) -> String {
    parsed(source)
        .items
        .iter()
        .map(ToTokens::to_token_stream)
        .map(|tokens| tokens.to_string())
        .filter(|tokens| {
            let compact = tokens.replace(' ', "");
            !compact.starts_with("#[cfg(test)]") && !compact.starts_with("#[cfg(all(test,")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_replicator_module(name: &str, source: &str) -> Result<(), String> {
    let production = production_items(source);
    for forbidden in [
        "RuntimeEffectAction",
        "AdmittedAuthority",
        "RuntimeSnapshot",
        "RuntimePostcondition",
        "ReplicaAuthorityStore",
        "ReplicaRuntimeInstruction",
        "TransitionKind",
    ] {
        if production.contains(forbidden) {
            return Err(format!("{name} contains forbidden {forbidden}"));
        }
    }
    Ok(())
}

fn validate_replicator_tree(root: &Path) -> Result<(), String> {
    for entry in fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if path.is_dir() {
            validate_replicator_tree(&path)?;
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let root_mod = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/replicator/mod.rs");
        if path == root_mod || name.contains("test") {
            continue;
        }
        let source = fs::read_to_string(&path).map_err(|error| error.to_string())?;
        validate_replicator_module(&path.display().to_string(), &source)?;
    }
    Ok(())
}

fn validate_managed_replica_runtime_boundary(
    replicator: &str,
    configuration: &str,
    runtime: &str,
    log: &str,
    quorum: &str,
    host: &str,
) -> Result<(), String> {
    let trait_body = replicator
        .split("pub(crate) trait ManagedReplicatorLifecycle")
        .nth(1)
        .and_then(|body| body.split("pub(crate) struct ManagedFenceGuard").next())
        .ok_or_else(|| "ManagedReplicatorLifecycle body was not found".to_string())?;
    for forbidden in [
        "RuntimeEffectAction",
        "AdmittedAuthority",
        "RuntimeSnapshot",
        "RuntimePostcondition",
        "apply_topology",
    ] {
        if trait_body.contains(forbidden) {
            return Err(format!(
                "managed replica-runtime boundary contains forbidden {forbidden}"
            ));
        }
    }
    for required in [
        "prepare_replica_configuration",
        "commit_replica_configuration",
        "synchronize_replica_configuration",
        "authorize_failover_prefix",
        "prepare_switchover",
        "prepare_secondary_removal",
        "observe_secondary_removal_witness",
        "observe_secondary_removal_progress",
        "accept_secondary_removal_commit",
        "accept_historical_secondary_removal_commit",
        "fence_retirement",
        "complete_retirement",
        "prepare_access",
        "publish_access",
        "observe_engine",
    ] {
        if !trait_body.contains(required) {
            return Err(format!(
                "managed replica-runtime boundary is missing explicit {required}"
            ));
        }
    }
    for outcome in [
        "ManagedRemovalPreparationOutcome",
        "ManagedRemovalWitnessOutcome",
        "ManagedRemovalProgressOutcome",
        "ManagedRemovalAcceptanceOutcome",
        "ManagedHistoricalRemovalAcceptanceOutcome",
        "ManagedRetirementFenceOutcome",
        "ManagedRetirementCompletionOutcome",
    ] {
        if !trait_body.contains(outcome) {
            return Err(format!(
                "managed replica-runtime boundary is missing stage-specific {outcome}"
            ));
        }
    }
    let parsed_replicator = parsed(replicator);
    let lifecycle_trait = parsed_replicator
        .items
        .iter()
        .find_map(|item| match item {
            Item::Trait(item) if item.ident == "ManagedReplicatorLifecycle" => Some(item),
            _ => None,
        })
        .ok_or_else(|| "ManagedReplicatorLifecycle syntax was not found".to_string())?;
    let method_outputs = lifecycle_trait
        .items
        .iter()
        .filter_map(|item| match item {
            TraitItem::Fn(method) => {
                Some((method.sig.ident.to_string(), compact(&method.sig.output)))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for (method, outcome) in [
        (
            "prepare_secondary_removal",
            "ManagedRemovalPreparationOutcome",
        ),
        (
            "observe_secondary_removal_witness",
            "ManagedRemovalWitnessOutcome",
        ),
        (
            "observe_secondary_removal_progress",
            "ManagedRemovalProgressOutcome",
        ),
        (
            "accept_secondary_removal_commit",
            "ManagedRemovalAcceptanceOutcome",
        ),
        (
            "accept_historical_secondary_removal_commit",
            "ManagedHistoricalRemovalAcceptanceOutcome",
        ),
        ("fence_retirement", "ManagedRetirementFenceOutcome"),
        ("complete_retirement", "ManagedRetirementCompletionOutcome"),
    ] {
        let expected = format!("->Result<{outcome}>");
        if method_outputs.get(method) != Some(&expected) {
            return Err(format!(
                "managed method {method} must return stage-specific {outcome}"
            ));
        }
    }
    let data_plane = replicator
        .split("pub(crate) trait ManagedReplicatorDataPlane")
        .nth(1)
        .and_then(|body| body.split("pub enum ReplicaSetQuorumMode").next())
        .ok_or_else(|| "ManagedReplicatorDataPlane body was not found".to_string())?;
    let store = replicator
        .split("pub(crate) trait ManagedReplicaStore")
        .nth(1)
        .and_then(|body| body.split("pub(crate) struct ManagedFenceGuard").next())
        .ok_or_else(|| "ManagedReplicaStore body was not found".to_string())?;
    for (name, body) in [
        ("ManagedReplicatorDataPlane", data_plane),
        ("ManagedReplicaStore", store),
    ] {
        for forbidden in [
            "RuntimeEffectAction",
            "AdmittedAuthority",
            "RuntimeSnapshot",
            "RuntimePostcondition",
        ] {
            if body.contains(forbidden) {
                return Err(format!("{name} contains forbidden {forbidden}"));
            }
        }
    }
    let production = [
        ("runtime.rs", production_items(runtime)),
        (
            "replicator/configuration.rs",
            production_items(configuration),
        ),
        ("replicator/log.rs", production_items(log)),
        ("replicator/quorum.rs", production_items(quorum)),
    ];
    for (name, body) in &production {
        for forbidden in [
            "RuntimeEffectAction",
            "AdmittedAuthority",
            "RuntimeSnapshot",
            "RuntimePostcondition",
            "ReplicaAuthorityStore",
            "ReplicaRuntimeInstruction",
            "TransitionKind",
        ] {
            if body.contains(forbidden) {
                return Err(format!("{name} contains forbidden {forbidden}"));
            }
        }
    }
    let managed_host = host
        .split("struct ManagedLifecycleBackend")
        .nth(1)
        .and_then(|body| body.split("struct CustomReplicatorHost").next())
        .ok_or_else(|| "ManagedLifecycleBackend body was not found".to_string())?;
    for forbidden in ["apply_topology", "execute_removal_action"] {
        if managed_host.contains(forbidden) {
            return Err(format!(
                "managed host compatibility routing contains forbidden {forbidden}"
            ));
        }
    }
    let admission = managed_host
        .split("async fn admit_authority")
        .nth(1)
        .and_then(|body| body.split("async fn register_peer_session").next())
        .ok_or_else(|| "managed authority admission body was not found".to_string())?;
    let ordered = [
        "prepare_replica_configuration",
        ".admit(&authority)",
        "commit_replica_configuration",
        "install_managed_authority",
        "synchronize_replica_configuration",
    ];
    let mut cursor = 0;
    for step in ordered {
        let offset = admission[cursor..]
            .find(step)
            .ok_or_else(|| format!("managed authority admission is missing ordered step {step}"))?;
        cursor += offset + step.len();
    }
    Ok(())
}

#[test]
fn lifecycle_capability_boundaries_are_narrow() {
    validate_project().unwrap();
}

#[test]
fn projection_capabilities_reject_complete_runtime_snapshots() {
    let hosting = source("src/host/hosting.rs");
    assert!(hosting.contains("fallback_snapshot: ReplicaRuntimeState"));
    let custom = source("src/host/custom.rs");
    assert!(custom.contains("state: Arc<RwLock<ReplicaRuntimeState>>"));
    for (start, end, projection) in [
        (
            "trait ReportHost",
            "pub(crate) struct ReportRuntime",
            "ReportObservation",
        ),
        (
            "trait BuildHost",
            "trait BuildAttemptHost",
            "BuildObservation",
        ),
        (
            "trait PeerDiscoveryHost",
            "pub(crate) struct PeerDiscoveryRuntime",
            "PeerObservation",
        ),
        (
            "trait OutboundHost",
            "pub(crate) struct OutboundRuntime",
            "OutboundObservation",
        ),
    ] {
        let body = hosting
            .split_once(start)
            .and_then(|(_, rest)| rest.split_once(end).map(|(body, _)| body))
            .unwrap_or_else(|| panic!("missing {start} capability"));
        assert!(
            !body.contains("RuntimeSnapshot"),
            "{start} must not transport RuntimeSnapshot"
        );
        assert!(
            body.contains(projection),
            "{start} must transport {projection}"
        );
    }

    let lifecycle = source("src/host/lifecycle.rs");
    let recovery = lifecycle
        .split_once("pub(super) struct RecoveryRuntime")
        .and_then(|(_, rest)| {
            rest.split_once("pub(super) struct AccessClosure")
                .map(|(body, _)| body)
        })
        .expect("RecoveryRuntime capability");
    assert!(
        !recovery.contains("RuntimeSnapshot"),
        "RecoveryRuntime must not transport RuntimeSnapshot"
    );
    assert!(recovery.contains("RecoveryObservation"));

    let report_lifecycle = lifecycle
        .split_once("pub(super) struct ReportObservationRuntime")
        .and_then(|(_, rest)| {
            rest.split_once("pub(super) struct EvidenceRuntime")
                .map(|(body, _)| body)
        })
        .expect("ReportLifecycle capability");
    assert!(
        !report_lifecycle.contains("RuntimeSnapshot"),
        "ReportObservationRuntime must not transport RuntimeSnapshot"
    );
    assert!(report_lifecycle.contains("ReportObservation"));
    let report_callback_forbidden = [
        "AccessLifecycle",
        "observe_progress",
        "refresh_progress",
        "reconcile_access",
        "run_access_transaction",
        "restored_access",
    ];
    for callback in report_callback_forbidden {
        assert!(
            !report_lifecycle.contains(callback),
            "ReportObservationRuntime must not expose callback {callback}"
        );
    }
    let injected_report_lifecycle = format!(
        "{report_lifecycle}\nfn injected() {{ self.observation.observe_progress().await; }}"
    );
    assert!(
        report_callback_forbidden
            .iter()
            .any(|callback| injected_report_lifecycle.contains(callback)),
        "report callback negative fixture must be rejected"
    );

    let report_runtime = hosting
        .split_once("pub(crate) struct ReportRuntime")
        .and_then(|(_, rest)| {
            rest.split_once("pub(crate) struct RecoveryOwnerRuntime")
                .map(|(body, _)| body)
        })
        .expect("ReportRuntime capability");
    for mutation in [
        "observe_progress",
        "reconcile_durable_access",
        "refresh_catch_up_capability",
    ] {
        assert!(
            !report_runtime.contains(mutation),
            "ReportRuntime must not expose lifecycle mutation {mutation}"
        );
    }
    let recovery_runtime = hosting
        .split_once("pub(crate) struct RecoveryOwnerRuntime")
        .and_then(|(_, rest)| rest.split_once("trait BuildHost").map(|(body, _)| body))
        .expect("RecoveryOwnerRuntime capability");
    for owned in [
        "observe_progress",
        "reconcile_durable_access",
        "refresh_catch_up_capability",
    ] {
        assert!(
            recovery_runtime.contains(owned),
            "RecoveryOwnerRuntime must own {owned}"
        );
    }
    for broad_projection in [
        "ReportHost::observation(self).await.build()",
        "ReportHost::observation(self).await.peer()",
        "ReportHost::observation(self).await.outbound()",
    ] {
        assert!(
            !hosting.contains(broad_projection),
            "narrow providers must not assemble the complete report observation through {broad_projection}"
        );
    }

    let report = source("src/host/report.rs");
    let production_report = production_items(&report);
    let report_mutations = [
        "record_partition_reports",
        "complete_application_initialization",
        "migrate_schema",
        "begin_effect",
        "mark_effect_applied",
        "complete_effect",
        "cancel_effect",
        "begin_configuration",
        "journal_build",
        "abandon_build",
        "advance_configuration",
        "complete_configuration",
        "set_reconfiguration",
        "clear_reconfiguration",
        "observe_progress",
        "reconcile_durable_access",
        "catch_up_capability().await",
        "tokio::time::sleep",
        "RuntimeError::",
        "RuntimeSnapshot",
    ];
    for mutation in report_mutations {
        assert!(
            !production_report.contains(mutation),
            "status reporting must not perform {mutation}"
        );
    }
    let injected_report = format!(
        "{production_report}\nasync fn injected(store: &impl AgentStore) {{ store.cancel_effect(todo!()).await; let _: RuntimeSnapshot = todo!(); }}"
    );
    assert!(
        report_mutations
            .iter()
            .filter(|mutation| { **mutation == "cancel_effect" || **mutation == "RuntimeSnapshot" })
            .all(|mutation| injected_report.contains(mutation)),
        "report mutation negative fixture must be rejected"
    );

    let service = source("src/host/service.rs");
    assert_eq!(
        service.matches(".report(&report_runtime)").count(),
        2,
        "status RPC and post-command status must share the read-only reporter"
    );
    assert!(
        service.contains("self.command_tasks.shutdown().await")
            && service
                .contains(".run(async move { coordinator.ensure_configuration(*command).await })")
            && service.contains(".run(async move { coordinator.ensure_build(*command).await })"),
        "configuration and build commands must remain service-owned through shutdown"
    );
    assert!(
        service.find("self.command_tasks.shutdown().await")
            < service.find("self.runtime.shutdown_configuration_work().await?"),
        "configuration command tasks must stop before configuration cleanup"
    );
    assert!(
        hosting.contains("if observation.engine.catch_up_capability.is_none()"),
        "managed engine catch-up capability must not be overwritten by the custom cache"
    );
    let observation = source("src/host/observation.rs");
    let production_observation = production_items(&observation);
    for serialization in [
        "Serialize",
        "Deserialize",
        "serde::Serialize",
        "serde::Deserialize",
    ] {
        assert!(
            !production_observation.contains(serialization),
            "process-local owner observations must not enter durable serialization via {serialization}"
        );
    }
    let durable_state = source("src/host/state.rs");
    for transient in [
        "HostProxyObservation",
        "ReplicationEngineObservation",
        "ReportObservation",
        "PendingAccessObservation",
        "ManagedOperationFence",
        "engine_session_id",
        "engine_generation",
        "host_generation",
        "diagnostic_revision",
    ] {
        assert!(
            !durable_state.contains(transient),
            "durable agent state must not contain transient observation {transient}"
        );
    }

    for consumer in [
        "src/host/custom.rs",
        "src/host/transport.rs",
        "src/host/recovery.rs",
        "src/host/report.rs",
    ] {
        let source = source(consumer);
        let production = production_items(&source);
        assert!(
            !production.contains("diagnostic_revision"),
            "{consumer} must not depend on unrelated engine diagnostics"
        );
    }
    let recovery_owner = production_items(&source("src/host/recovery.rs"));
    for scheduling_boundary in [
        "RecoverySchedule",
        "custom_tick",
        "schedule . wait_for_timer",
    ] {
        assert!(
            recovery_owner.contains(scheduling_boundary),
            "recovery scheduling must retain the explicit {scheduling_boundary} boundary"
        );
    }
}

#[test]
fn effect_completion_contract_is_narrow_and_action_owned() {
    let effects = source("src/effects.rs");
    let result = effects
        .split_once("pub(crate) struct RuntimeEffectResult")
        .and_then(|(_, rest)| {
            rest.split_once("impl RuntimeEffectResult")
                .map(|(body, _)| body)
        })
        .expect("RuntimeEffectResult definition");
    assert!(result.contains("RuntimeEffectOutcome"));
    for forbidden in ["RuntimeSnapshot", "RuntimePostcondition", "postcondition"] {
        assert!(
            !result.contains(forbidden),
            "effect result must not contain broad correctness field {forbidden}"
        );
    }
    assert!(
        effects.contains("impl RuntimeEffectAction")
            && effects.contains("fn completion_family(&self) -> RuntimeEffectFamily")
            && effects.contains("impl RuntimeEffectOutcome"),
        "every action and outcome must participate in an exhaustive family mapping"
    );

    let lifecycle = source("src/host/lifecycle.rs");
    let evidence = lifecycle
        .split_once("pub(super) struct EffectEvidenceRuntime")
        .and_then(|(_, rest)| rest.split_once("#[cfg(test)]").map(|(body, _)| body))
        .expect("EffectEvidenceRuntime body");
    assert!(evidence.contains("effect_outcome"));
    assert!(!evidence.contains("snapshot("));
    assert!(!evidence.contains("postcondition"));

    let hosting = source("src/host/hosting.rs");
    let prepare = hosting
        .split_once("async fn prepare_effect")
        .and_then(|(_, rest)| {
            rest.split_once("async fn consume_cancelled_build_effect")
                .map(|(body, _)| body)
        })
        .expect("prepare_effect body");
    for forbidden in [
        "snapshot_postcondition",
        "RuntimePostcondition",
        "self.snapshot().await.into()",
    ] {
        assert!(
            !prepare.contains(forbidden),
            "effect execution must not derive completion through {forbidden}"
        );
    }
    assert!(hosting.contains("effect_outcome("));

    let store = source("src/host/sqlite_store.rs");
    let completion = store
        .split_once("async fn complete_effect")
        .and_then(|(_, rest)| {
            rest.split_once("async fn cancel_effect")
                .map(|(body, _)| body)
        })
        .expect("complete_effect body");
    assert!(completion.contains("match (&pending.effect.action, &result.outcome)"));
    for broad_copy in [
        "state.role = result.",
        "state.read_status = result.",
        "state.write_status = result.",
        "state.current_configuration = result.",
        "state.build_progress = result.",
    ] {
        assert!(
            !completion.contains(broad_copy),
            "durable completion must not use unconditional broad copy {broad_copy}"
        );
    }
    assert!(
        completion.contains("retained.result == *result"),
        "exact duplicate retained completion must be explicitly idempotent"
    );
}

#[test]
fn managed_replica_runtime_boundary_is_typed() {
    let replicator = source("src/replicator/mod.rs");
    let configuration = source("src/replicator/configuration.rs");
    let runtime = source("src/runtime.rs");
    let log = source("src/replicator/log.rs");
    let quorum = source("src/replicator/quorum.rs");
    let host = source("src/host/custom.rs");
    validate_managed_replica_runtime_boundary(
        &replicator,
        &configuration,
        &runtime,
        &log,
        &quorum,
        &host,
    )
    .unwrap();
    validate_replicator_tree(&PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/replicator"))
        .unwrap();

    let leaked_trait = replicator.replace(
        "configuration: ManagedReplicaConfiguration",
        "configuration: AdmittedAuthority",
    );
    assert_rejected(
        validate_managed_replica_runtime_boundary(
            &leaked_trait,
            &configuration,
            &runtime,
            &log,
            &quorum,
            &host,
        ),
        "AdmittedAuthority",
    );

    let leaked_runtime = format!("{runtime}\nuse crate::effects::RuntimeEffectAction;\n");
    assert_rejected(
        validate_managed_replica_runtime_boundary(
            &replicator,
            &configuration,
            &leaked_runtime,
            &log,
            &quorum,
            &host,
        ),
        "RuntimeEffectAction",
    );

    let leaked_data_plane = replicator.replace(
        "async fn next_outbound_item(&self) -> Option<OutboundOperation>;",
        "async fn next_outbound_item(&self) -> Option<OutboundOperation>;\n    async fn leak(&self, action: RuntimeEffectAction);",
    );
    assert_rejected(
        validate_managed_replica_runtime_boundary(
            &leaked_data_plane,
            &configuration,
            &runtime,
            &log,
            &quorum,
            &host,
        ),
        "RuntimeEffectAction",
    );

    let leaked_after_test = format!(
        "{quorum}\n#[cfg(not(test))]\nmod leaked_production {{ use crate::effects::RuntimeEffectAction; }}\n"
    );
    assert_rejected(
        validate_managed_replica_runtime_boundary(
            &replicator,
            &configuration,
            &runtime,
            &log,
            &leaked_after_test,
            &host,
        ),
        "RuntimeEffectAction",
    );

    assert_rejected(
        validate_replicator_module(
            "replicator/sender.rs",
            "use crate::effects::RuntimeEffectAction;",
        ),
        "RuntimeEffectAction",
    );

    let leaked_configuration =
        format!("{configuration}\nuse crate::protocol::types::TransitionKind;\n");
    assert_rejected(
        validate_managed_replica_runtime_boundary(
            &replicator,
            &leaked_configuration,
            &runtime,
            &log,
            &quorum,
            &host,
        ),
        "TransitionKind",
    );

    let broad_removal = replicator.replace(
        "ManagedRemovalWitnessOutcome",
        "ManagedRemovalPreparationOutcome",
    );
    assert_rejected(
        validate_managed_replica_runtime_boundary(
            &broad_removal,
            &configuration,
            &runtime,
            &log,
            &quorum,
            &host,
        ),
        "ManagedRemovalWitnessOutcome",
    );
}

#[test]
#[rustfmt::skip]
fn lifecycle_capability_guard_rejects_representative_escapes() {
    reject_policy("trait ProcessLifecycle {}\ntrait BuildLifecycle {}\ntrait UniversalLifecycle: ProcessLifecycle + BuildLifecycle {}", false, "UniversalLifecycle");

    let lifecycle = source("src/host/lifecycle.rs");
    let inherited_capability = lifecycle.replace(
        "pub(super) trait ProcessLifecycle: Send + Sync {",
        "pub(super) trait ProcessLifecycle: Send + Sync + BuildLifecycle {",
    );
    assert_rejected(validate_module_policy(&parsed(&inherited_capability), &[], true), "ProcessLifecycle");

    let trait_escape = lifecycle.replace(
        "    async fn settle_primary_prefix(&self) -> Result<()>;\n",
        "    async fn settle_primary_prefix(&self) -> Result<()>;\n    fn build(&self) -> Arc<dyn self::BuildLifecycle>;\n",
    );
    assert_ne!(trait_escape, lifecycle, "trait mutation anchor must match the source");
    assert_rejected(validate_module_policy(&parsed(&trait_escape), &[], true), "ProcessLifecycle::build");

    let harmless_method = format!(
        "{lifecycle}\nimpl ProcessRuntime {{ fn harmless_narrow_helper(&self) -> bool {{ true }} }}\n"
    );
    validate_module_policy(&parsed(&harmless_method), allowed_aggregates(Path::new("lifecycle.rs")), true).unwrap();

    reject_policy("use self::BuildRuntime as B;\ntype R = ReportRuntime;\nstruct Broker { build: B, report: R }", false, "guarded");
    reject_policy("use self::BuildLifecycle as Other;", false, "guarded import alias");
    reject_policy("struct Broker { build: BuildRuntime, report: ReportRuntime }\nfn local() { struct Local(BuildRuntime, ReportRuntime); }", false, "aggregate");
    reject_policy("struct UniversalFacade { host: Arc<RuntimeHost> }", true, "UniversalFacade");
    reject_policy("fn all_views() -> (BuildRuntime, ReportRuntime) { unreachable!() }", false, "all_views");

    let view_escape = format!(
        "{lifecycle}\nimpl ProcessRuntime {{ fn build_capability(&self) -> Arc<dyn self::BuildLifecycle> {{ unreachable!() }} }}\n"
    );
    assert_rejected(validate_module_policy(&parsed(&view_escape), allowed_aggregates(Path::new("lifecycle.rs")), true), "build_capability");

    let conversion = format!(
        "{lifecycle}\nimpl std::ops::Deref for ProcessRuntime {{ type Target = RuntimeHost; fn deref(&self) -> &Self::Target {{ unreachable!() }} }}\n"
    );
    assert_rejected(validate_module_policy(&parsed(&conversion), allowed_aggregates(Path::new("lifecycle.rs")), true), "conversion");

    reject_policy("trait ReportHost { fn broad(&self) -> &PodRuntime; }", true, "ReportHost::broad");
    let host_view_escape = syn::parse_file(
        "struct ReportRuntime { inner: Arc<dyn ReportHost>, build: Arc<dyn BuildHost> }",
    )
    .unwrap();
    assert_rejected(
        validate_view_boundaries(&host_view_escape, &[("ReportRuntime", &["dynReportHost"])]),
        "ReportRuntime",
    );
    reject_policy("trait ReportExtension: BuildHost {}\ntrait ReportHost: ReportExtension {}", true, "transitively");
    reject_policy("trait ProcessExtension: ProcessLifecycle {} trait BuildExtension: BuildLifecycle {} trait RenamedFacade: ProcessExtension + BuildExtension {}", true, "RenamedFacade");
    reject_policy("mod nested { trait ProcessExtension: ProcessLifecycle {} trait BuildExtension: BuildLifecycle {} trait RenamedFacade: ProcessExtension + BuildExtension {} }", true, "RenamedFacade");
    reject_policy("trait ReportHost { fn primary(&self) -> Arc<dyn PrimaryReplicator>; }", true, "ReportHost::primary");
    reject_policy("type DirectPrimary = dyn PrimaryReplicator;", true, "DirectPrimary");
    reject_policy("#[allow(clippy::disallowed_types)] mod worker { struct Facade { runtime: Arc<PodRuntime> } }", false, "Facade");
    reject_policy("#[deny(clippy::disallowed_types)] mod worker { #![allow(clippy::disallowed_types)] struct Facade { runtime: Arc<PodRuntime> } }", false, "Facade");
    reject_policy("#[allow(clippy::disallowed_types)] struct UnauthorizedFacade { runtime: Arc<PodRuntime> }", false, "UnauthorizedFacade");

    for gate in [
        "#[cfg(all())]",
        "#[cfg(not(windows))]",
        "#[cfg(kuberic_workspace_tests)]",
    ] {
        let mutation = lifecycle.replacen(
            "#[cfg(any(all(test, kuberic_workspace_tests), feature = \"testing\"))]",
            gate,
            1,
        );
        assert_rejected(validate_fixture_gates(&parsed(&mutation)), "production");
    }

    reject_transport("fn dispatch(runtime: BuildRuntime) { runtime.execute_admitted_build(); runtime.execute_admitted_build(); }", "2 admitted-build");
    reject_transport("fn dispatch(runtime: BuildRuntime, primary: PrimaryReplicator) { runtime.execute_admitted_build(); primary.build_replica(); }", "direct primary");
    reject_transport("fn dispatch(runtime: BuildRuntime) { runtime.execute_admitted_build(); BuildRuntime::execute_admitted_build(&runtime); }", "2 admitted-build");
    reject_transport("fn dispatch(runtime: BuildRuntime) { runtime.execute_admitted_build(); (BuildRuntime::execute_admitted_build)(&runtime); }", "2 admitted-build");
    reject_transport("fn dispatch(runtime: BuildRuntime, primary: PrimaryReplicator) { runtime.execute_admitted_build(); PrimaryReplicator::build_replica(&primary); }", "direct primary");
    validate_transport_routing(
        "fn production(r: BuildRuntime) { r.execute_admitted_build(); #[cfg(test)] let _x = r.execute_admitted_build(); match true { #[cfg(test)] true => r.execute_admitted_build(), _ => {} } tracing::debug!(\"build_replica retry\"); }",
    )
    .unwrap();
    reject_transport("#[cfg(test)] fn test_only(runtime: BuildRuntime) { runtime.execute_admitted_build(); }", "0 admitted-build");

    let custom = source("src/host/custom.rs");
    let wiring_getter = custom.replace(
        "impl ReplicatorLifecycleRegistration {\n",
        "impl ReplicatorLifecycleRegistration {\n    fn all_capabilities(&self) -> &super::lifecycle::LifecycleWiring { unreachable!() }\n",
    );
    assert_rejected(validate_module_policy(&parsed(&wiring_getter), allowed_aggregates(Path::new("custom.rs")), true), "all_capabilities");
    reject_policy("impl From<ReplicatorLifecycleRegistration> for LifecycleWiring { fn from(value: ReplicatorLifecycleRegistration) -> Self { unreachable!() } }", true, "conversion");
    reject_policy("impl From<RegisteredReplicator> for LifecycleWiring { fn from(value: RegisteredReplicator) -> Self { unreachable!() } }", true, "conversion");
    reject_policy("impl From<ReplicatorLifecycleRegistration> for ManagedLifecycleBackend { fn from(value: ReplicatorLifecycleRegistration) -> Self { unreachable!() } }", true, "conversion");
    reject_policy("mod nested { impl RegisteredReplicator { fn leak(&self) -> LifecycleWiring { unreachable!() } } }", true, "leak");
    reject_policy("mod nested { impl ReplicatorLifecycleRegistration { fn leak(&self) -> LifecycleWiring { unreachable!() } } }", true, "leak");
    assert_rejected(validate_production_module(Path::new("hosting/worker.rs"), "impl ReportRuntime { fn leak(&self) -> BuildRuntime { unreachable!() } }", true), "leak");

    reject_cancellation("struct BuildRuntimeCancellation;\nimpl BuildRuntimeCancellation { fn new(runtime: BuildRuntime) -> Self { unreachable!() } }", "cancellation-only");
    reject_cancellation("struct BuildRuntimeCancellation;\nimpl BuildRuntimeCancellation { fn new(runtime: BuildAttemptRuntime) -> Self { let host: Option<Arc<PodRuntime>> = None; unreachable!() } }", "broader runtime");
    reject_cancellation("struct BuildRuntimeCancellation; impl BuildRuntimeCancellation { fn new(runtime: BuildAttemptRuntime) -> Self { let host = PodRuntime::new(); unreachable!() } }", "broader runtime");
    reject_cancellation("struct BuildRuntimeCancellation; impl BuildRuntimeCancellation { fn new(runtime: BuildAttemptRuntime, primary: Arc<dyn PrimaryReplicator>) -> Self { unreachable!() } }", "cancellation-only");

    let module_root = tempfile::tempdir().unwrap();
    let hosting_dir = module_root.path().join("hosting");
    fs::create_dir(&hosting_dir).unwrap();
    fs::write(
        module_root.path().join("mod.rs"),
        "#[allow(clippy::disallowed_types)] mod hosting; mod transport; #[allow(clippy::disallowed_types)] #[path = \".\"] mod helpers { mod root_worker; }",
    )
    .unwrap();
    fs::write(module_root.path().join("hosting.rs"), "mod worker;").unwrap();
    fs::write(
        hosting_dir.join("worker.rs"),
        "struct NestedFacade { runtime: Arc<PodRuntime> }",
    )
    .unwrap();
    let inline_worker = module_root.path().join("transport/helpers");
    fs::create_dir_all(&inline_worker).unwrap();
    fs::write(module_root.path().join("transport.rs"), "#[allow(clippy::disallowed_types)] mod helpers { mod worker; }").unwrap();
    fs::write(inline_worker.join("worker.rs"), "struct InlineFacade { runtime: Arc<PodRuntime> }").unwrap();
    fs::write(module_root.path().join("root_worker.rs"), "struct RootFacade { runtime: Arc<PodRuntime> }").unwrap();
    let relative = Path::new("hosting/worker.rs");
    let modules = production_modules(module_root.path());
    assert!(modules.contains(&(relative.to_path_buf(), true)));
    let source = fs::read_to_string(module_root.path().join(relative)).unwrap();
    assert_rejected(
        validate_production_module(relative, &source, true),
        "hosting/worker.rs",
    );
    assert!(modules.contains(&(PathBuf::from("transport/helpers/worker.rs"), true)));
    let inline = fs::read_to_string(module_root.path().join("transport/helpers/worker.rs")).unwrap();
    assert_rejected(validate_production_module(Path::new("transport/helpers/worker.rs"), &inline, true), "InlineFacade");
    assert!(modules.contains(&(PathBuf::from("root_worker.rs"), true)));

    let nested_view = format!("{lifecycle}\nmod nested {{ impl super::ProcessRuntime {{ fn leak(&self) -> super::BuildLifecycleRuntime {{ unreachable!() }} }} }}");
    assert_rejected(validate_module_policy(&parsed(&nested_view), allowed_aggregates(Path::new("lifecycle.rs")), true), "leak");
}
