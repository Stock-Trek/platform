//! Local fork of `traitreg-macros` 0.4.0.
//!
//! The only behavioural changes are that registry constructors are placed in
//! the wasm `.init_array` and that the registry is built lazily on first use.
//! Both are required for algorithms registered by user code to be visible when
//! the runner executes as a WebAssembly guest.

use quote::quote;

use parse::{RegisterAttribute, RegisterItem, RegistryAttribute, RegistryItem};

mod parse;

/// Register an implementation of a trait on a concrete type.
///
/// ```rust
/// trait MyTrait {}
/// struct MyType;
///
/// #[traitreg::register]
/// impl MyTrait for MyType {}
/// ```
///
/// Supports registration of a constructor, which can be any associated method with the signature
/// `fn() -> Self`. For Example:
///
/// ```rust
/// trait MyTrait {}
///
/// #[derive(Default)]
/// struct MyType;
///
/// #[traitreg::register(default)]
/// impl MyTrait for MyType {}
///
/// struct MyOtherType;
/// impl MyOtherType {
///     fn new() -> Self { Self }
/// }
///
/// #[traitreg::register(new)]
/// impl MyTrait for MyOtherType {}
/// ```
#[proc_macro_attribute]
pub fn register(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    // Read custom / default constructor from attribute if it exists
    let constructor_fn = if attr.is_empty() {
        None
    } else {
        Some(syn::parse_macro_input!(attr as RegisterAttribute))
    };

    let has_constructor = constructor_fn.is_some();
    let has_constructor = quote! { #has_constructor };

    let constructor_fn_call_str = if let Some(cfn) = constructor_fn {
        let ident = cfn.constructor_fn_ident;
        quote! {
            Some(Box::new(Self::#ident()))
        }
    } else {
        quote! {
            None
        }
    };

    let item_clone = item.clone();

    let parsed_item = syn::parse_macro_input!(item as RegisterItem);

    let (trait_not, trait_path, _) = parsed_item
        .item
        .trait_
        .as_ref()
        .expect("Can only register an implementation of a trait, 'impl <Trait> for <Type>'.");
    assert!(
        trait_not.is_none(),
        "Cannot register inverted impl trait: 'impl !Trait for Type'."
    );

    let trait_ident = trait_path
        .require_ident()
        .expect("Expected trait in impl block to have an identifier.");
    let trait_name = format!("{trait_ident}");

    let type_path = parsed_item.self_type_path();
    let type_ident = type_path
        .require_ident()
        .expect("Expected type in impl block to have an identifier.");
    let type_name = format!("{type_ident}");

    let register_static_ident =
        syn::parse_str::<syn::Ident>(format!("{}_{}__Register", type_ident, trait_ident).as_ref())
            .expect("Unable to create identifier");
    let register_static_fn_ident = syn::parse_str::<syn::Ident>(
        format!("{}_{}__RegisterFn", type_ident, trait_ident).as_ref(),
    )
    .expect("Unable to create identifier");

    let mut result: proc_macro::TokenStream = quote! {
        impl traitreg::RegisteredImpl<Box<dyn #trait_path>> for #type_path {
            const INSTANCIATE: fn() -> Option<Box<dyn #trait_path>> = || { #constructor_fn_call_str };
            const HAS_CONSTRUCTOR: bool = #has_constructor;
            const NAME: &'static str = #type_name;
            const PATH: &'static str = stringify!(#type_path);
            const FILE: &'static str = ::core::file!() ;
            const MODULE_PATH: &'static str = ::core::module_path!();
            const TRAIT_NAME: &'static str = #trait_name;
        }

        #[used]
        #[cfg_attr(any(target_os = "linux", target_os = "android"), unsafe(link_section = ".init_array.10000"))]
        #[cfg_attr(target_os = "freebsd", unsafe(link_section = ".init_array.10000"))]
        #[cfg_attr(target_os = "netbsd", unsafe(link_section = ".init_array.10000"))]
        #[cfg_attr(target_os = "openbsd", unsafe(link_section = ".init_array.10000"))]
        #[cfg_attr(target_os = "dragonfly", unsafe(link_section = ".init_array.10000"))]
        #[cfg_attr(target_os = "illumos", unsafe(link_section = ".init_array.10000"))]
        #[cfg_attr(target_os = "haiku", unsafe(link_section = ".init_array.10000"))]
        #[cfg_attr(target_vendor = "apple", unsafe(link_section = "__DATA,__mod_init_func"))]
        #[cfg_attr(target_family = "wasm", unsafe(link_section = ".init_array"))]
        #[cfg_attr(windows, unsafe(link_section = ".CRT$XCT"))]
        static #register_static_ident: extern fn() = {
            extern fn #register_static_fn_ident() {
                traitreg::__register_impl::<Box<dyn #trait_path>, #type_path>();
            }
            #register_static_fn_ident
        };
    }.into();

    result.extend(item_clone.clone());

    result
}

/// Create a registry of implementations of a trait
///
/// ```rust
/// trait MyTrait {}
///
/// #[traitreg::registry(MyTrait)]
/// static MYTRAIT_REGISTRY: () = ();
/// ```
#[proc_macro_attribute]
pub fn registry(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let registry_attr = syn::parse_macro_input!(attr as RegistryAttribute);
    let registry_item = syn::parse_macro_input!(item as RegistryItem);

    let trait_ident = registry_attr.trait_ident;
    let item = registry_item.item;

    let trait_name = format!("{trait_ident}");
    let item_ident = item.ident;
    let storage_ident = syn::parse_str::<syn::Ident>(format!("{}__STORAGE", item_ident).as_ref())
        .expect("Unable to create identifier");
    let wrapper_struct_ident =
        syn::parse_str::<syn::Ident>(format!("{}__TraitReg", item_ident).as_ref())
            .expect("Unable to create identifier");
    quote! {
        // Build lazily so that constructor ordering (which differs between ELF
        // priority sections and wasm `.init_array`) cannot produce an empty
        // registry before the implementations have registered themselves.
        static #storage_ident: ::std::sync::OnceLock<traitreg::TraitRegStorage<Box<dyn #trait_ident>>> =
            ::std::sync::OnceLock::new();

        static #item_ident: #wrapper_struct_ident = #wrapper_struct_ident {};

        struct #wrapper_struct_ident;

        impl ::core::ops::Deref for #wrapper_struct_ident {
            type Target = traitreg::TraitRegStorage<Box<dyn #trait_ident>>;
            fn deref(&self) -> &'static traitreg::TraitRegStorage<Box<dyn #trait_ident>> {
                #storage_ident.get_or_init(|| {
                    traitreg::TraitRegStorage::<Box<dyn #trait_ident>>::__new(#trait_name)
                })
            }
        }
    }.into()
}
