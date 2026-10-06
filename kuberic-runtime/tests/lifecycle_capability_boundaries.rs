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
    "ReportLifecycle", "EvidenceRuntime", "EffectEvidenceRuntime", "BuildLifecycleRuntime",
    "BuildCancellationRuntime", "OutboundLifecycleRuntime", "RemovalWitnessRuntime",
    "TopologyRuntime", "RecoveryRuntime", "ReportRuntime", "BuildRuntime",
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
    ("ReportLifecycle", &["dynAccessLifecycle", "dynLifecycleObservation"]),
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
    ("BuildRuntime", &["dynBuildHost", "BuildAttemptRuntime"]),
    ("BuildAttemptRuntime", &["dynBuildAttemptHost"]),
    ("PeerDiscoveryRuntime", &["dynPeerDiscoveryHost"]),
    ("OutboundRuntime", &["dynOutboundHost"]),
];

fn source(path: &str) -> String {
    fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)).unwrap()
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
            "ReportLifecycle",
            "BuildLifecycleRuntime",
            "RecoveryRuntime",
        ],
        Some("custom.rs") => &[
            "ReplicatorLifecycleRegistration",
            "AcceptedAccessEffect",
            "ManagedLifecycleBackend",
            "CustomReplicatorHost",
        ],
        Some("hosting.rs") => &[
            "RegisteredReplicator",
            "HostedPrimaryReplicator",
            "BuildRuntime",
            "PodRuntime",
            "RuntimeDataPlane",
            "RuntimeHost",
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

#[test]
fn lifecycle_capability_boundaries_are_narrow() {
    validate_project().unwrap();
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
