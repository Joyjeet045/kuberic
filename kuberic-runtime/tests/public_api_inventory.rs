use std::fs;
use std::path::{Path, PathBuf};

use quote::ToTokens;
use syn::{Attribute, Fields, ImplItem, Item, ItemMod, Meta, TraitItem, Visibility, parse_file};

const ALLOWLIST: &str = include_str!("../../scripts/runtime_source_public_api.allowlist");

#[test]
fn source_public_api_matches_reviewed_inventory() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let source = manifest_dir.join("src/lib.rs");
    let mut inventory = Vec::new();
    inventory_file("crate", &source, true, false, &mut inventory);
    inventory.sort();
    inventory.dedup();
    let actual = format!("{}\n", inventory.join("\n"));

    let allowlist_path = manifest_dir
        .parent()
        .expect("workspace root")
        .join("scripts/runtime_source_public_api.allowlist");
    if std::env::var_os("UPDATE_RUNTIME_SOURCE_API").is_some() {
        fs::write(&allowlist_path, &actual).expect("write source API allowlist");
        return;
    }

    assert_eq!(
        actual, ALLOWLIST,
        "kuberic-runtime source-public API changed; review the diff and, if intentional, run \
         UPDATE_RUNTIME_SOURCE_API=1 cargo test -p kuberic-runtime \
         --test public_api_inventory"
    );
}

fn inventory_file(
    module: &str,
    path: &Path,
    module_reachable: bool,
    module_hidden: bool,
    inventory: &mut Vec<String>,
) {
    let source =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let file =
        parse_file(&source).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()));
    for item in file.items {
        if cfg_test(item_attrs(&item)) {
            continue;
        }
        inventory_item(
            module,
            path,
            module_reachable,
            module_hidden,
            item,
            inventory,
        );
    }
}

fn inventory_item(
    module: &str,
    source_path: &Path,
    module_reachable: bool,
    module_hidden: bool,
    item: Item,
    inventory: &mut Vec<String>,
) {
    match item {
        Item::Const(item) if is_public(&item.vis) => record(
            inventory,
            module_reachable,
            module_hidden || doc_hidden(&item.attrs),
            format!(
                "const {module}::{}: {}",
                item.ident,
                tokens(item.ty.as_ref())
            ),
        ),
        Item::Enum(item) if is_public(&item.vis) => {
            let hidden = module_hidden || doc_hidden(&item.attrs);
            record(
                inventory,
                module_reachable,
                hidden,
                format!("enum {module}::{}{}", item.ident, tokens(&item.generics)),
            );
            for variant in item.variants {
                record(
                    inventory,
                    module_reachable,
                    hidden || doc_hidden(&variant.attrs),
                    format!(
                        "variant {module}::{}::{}{}",
                        item.ident,
                        variant.ident,
                        fields(&variant.fields)
                    ),
                );
            }
        }
        Item::ExternCrate(item) if is_public(&item.vis) => record(
            inventory,
            module_reachable,
            module_hidden || doc_hidden(&item.attrs),
            format!("extern crate {module}::{}", item.ident),
        ),
        Item::Fn(item) if is_public(&item.vis) => record(
            inventory,
            module_reachable,
            module_hidden || doc_hidden(&item.attrs),
            format!("fn {module}::{}", tokens(&item.sig)),
        ),
        Item::Impl(item) => {
            let owner = tokens(item.self_ty.as_ref());
            for member in item.items {
                match member {
                    ImplItem::Const(member) if is_public(&member.vis) => record(
                        inventory,
                        module_reachable,
                        module_hidden || doc_hidden(&member.attrs),
                        format!(
                            "impl {module}::{owner}::const {}: {}",
                            member.ident,
                            tokens(&member.ty)
                        ),
                    ),
                    ImplItem::Fn(member) if is_public(&member.vis) => record(
                        inventory,
                        module_reachable,
                        module_hidden || doc_hidden(&member.attrs),
                        format!("impl {module}::{owner}::{}", tokens(&member.sig)),
                    ),
                    ImplItem::Type(member) if is_public(&member.vis) => record(
                        inventory,
                        module_reachable,
                        module_hidden || doc_hidden(&member.attrs),
                        format!(
                            "impl {module}::{owner}::type {} = {}",
                            member.ident,
                            tokens(&member.ty)
                        ),
                    ),
                    _ => {}
                }
            }
        }
        Item::Mod(item) => inventory_module(
            module,
            source_path,
            module_reachable,
            module_hidden,
            item,
            inventory,
        ),
        Item::Static(item) if is_public(&item.vis) => record(
            inventory,
            module_reachable,
            module_hidden || doc_hidden(&item.attrs),
            format!(
                "static {module}::{}: {}",
                item.ident,
                tokens(item.ty.as_ref())
            ),
        ),
        Item::Struct(item) if is_public(&item.vis) => {
            let hidden = module_hidden || doc_hidden(&item.attrs);
            record(
                inventory,
                module_reachable,
                hidden,
                format!("struct {module}::{}{}", item.ident, tokens(&item.generics)),
            );
            for (index, field) in item.fields.iter().enumerate() {
                if is_public(&field.vis) {
                    let name = field
                        .ident
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| index.to_string());
                    record(
                        inventory,
                        module_reachable,
                        hidden || doc_hidden(&field.attrs),
                        format!(
                            "field {module}::{}::{name}: {}",
                            item.ident,
                            tokens(&field.ty)
                        ),
                    );
                }
            }
        }
        Item::Trait(item) if is_public(&item.vis) => {
            let hidden = module_hidden || doc_hidden(&item.attrs);
            record(
                inventory,
                module_reachable,
                hidden,
                format!(
                    "trait {module}::{}{}: {}",
                    item.ident,
                    tokens(&item.generics),
                    tokens(&item.supertraits)
                ),
            );
            for member in item.items {
                match member {
                    TraitItem::Const(member) => record(
                        inventory,
                        module_reachable,
                        hidden || doc_hidden(&member.attrs),
                        format!(
                            "trait {module}::{}::const {}: {}",
                            item.ident,
                            member.ident,
                            tokens(&member.ty)
                        ),
                    ),
                    TraitItem::Fn(member) => record(
                        inventory,
                        module_reachable,
                        hidden || doc_hidden(&member.attrs),
                        format!("trait {module}::{}::{}", item.ident, tokens(&member.sig)),
                    ),
                    TraitItem::Type(member) => record(
                        inventory,
                        module_reachable,
                        hidden || doc_hidden(&member.attrs),
                        format!(
                            "trait {module}::{}::type {}: {}",
                            item.ident,
                            member.ident,
                            tokens(&member.bounds)
                        ),
                    ),
                    _ => {}
                }
            }
        }
        Item::Type(item) if is_public(&item.vis) => record(
            inventory,
            module_reachable,
            module_hidden || doc_hidden(&item.attrs),
            format!(
                "type {module}::{}{} = {}",
                item.ident,
                tokens(&item.generics),
                tokens(item.ty.as_ref())
            ),
        ),
        Item::Union(item) if is_public(&item.vis) => record(
            inventory,
            module_reachable,
            module_hidden || doc_hidden(&item.attrs),
            format!("union {module}::{}{}", item.ident, tokens(&item.generics)),
        ),
        Item::Use(item) if is_public(&item.vis) => record(
            inventory,
            module_reachable,
            module_hidden || doc_hidden(&item.attrs),
            format!("use {module}::{}", tokens(&item.tree)),
        ),
        _ => {}
    }
}

fn inventory_module(
    parent: &str,
    source_path: &Path,
    parent_reachable: bool,
    parent_hidden: bool,
    item: ItemMod,
    inventory: &mut Vec<String>,
) {
    let reachable = parent_reachable && is_public(&item.vis);
    let hidden = parent_hidden || doc_hidden(&item.attrs);
    let module = format!("{parent}::{}", item.ident);
    if is_public(&item.vis) {
        record(
            inventory,
            parent_reachable,
            hidden,
            format!("module {module}"),
        );
    }

    if let Some((_, items)) = item.content {
        for item in items {
            if !cfg_test(item_attrs(&item)) {
                inventory_item(&module, source_path, reachable, hidden, item, inventory);
            }
        }
        return;
    }

    let path = module_path(source_path, &item);
    inventory_file(&module, &path, reachable, hidden, inventory);
}

fn module_path(source_path: &Path, item: &ItemMod) -> PathBuf {
    for attribute in &item.attrs {
        if attribute.path().is_ident("path")
            && let Meta::NameValue(name_value) = &attribute.meta
            && let syn::Expr::Lit(expression) = &name_value.value
            && let syn::Lit::Str(path) = &expression.lit
        {
            return source_path
                .parent()
                .expect("source parent")
                .join(path.value());
        }
    }

    let parent = source_path.parent().expect("source parent");
    let base = match source_path.file_name().and_then(|name| name.to_str()) {
        Some("lib.rs" | "main.rs" | "mod.rs") => parent.to_path_buf(),
        _ => parent.join(source_path.file_stem().expect("module source stem")),
    };
    let direct = base.join(format!("{}.rs", item.ident));
    if direct.is_file() {
        direct
    } else {
        base.join(item.ident.to_string()).join("mod.rs")
    }
}

fn record(inventory: &mut Vec<String>, reachable: bool, hidden: bool, signature: String) {
    inventory.push(format!(
        "{} {} {signature}",
        if reachable {
            "public-path"
        } else {
            "private-path"
        },
        if hidden { "hidden" } else { "documented" },
    ));
}

fn fields(fields: &Fields) -> String {
    match fields {
        Fields::Named(fields) => format!(" {{ {} }}", tokens(fields)),
        Fields::Unnamed(fields) => format!("({})", tokens(fields)),
        Fields::Unit => String::new(),
    }
}

fn is_public(visibility: &Visibility) -> bool {
    matches!(visibility, Visibility::Public(_))
}

fn doc_hidden(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("doc")
            && attribute
                .meta
                .to_token_stream()
                .to_string()
                .contains("hidden")
    })
}

fn cfg_test(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute.path().is_ident("cfg")
            && attribute
                .meta
                .to_token_stream()
                .to_string()
                .contains("test")
    })
}

fn item_attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        Item::Verbatim(_) => &[],
        _ => &[],
    }
}

fn tokens(value: &impl ToTokens) -> String {
    value.to_token_stream().to_string()
}
