#![feature(proc_macro_quote)]

#[macro_use]
extern crate quote;
extern crate syn;

use syn::{AttributeArgs, ItemFn, FnArg, PatType, parse_macro_input};
use proc_macro::{TokenStream};
use std::ops::Deref;

#[proc_macro_attribute]
pub fn redis_test(args: TokenStream, input: TokenStream) -> TokenStream {
    let args: AttributeArgs = parse_macro_input!(args);
    let parsed_fn: ItemFn = parse_macro_input!(input);

    let mut loaded_module = false;
    for arg in args {
        if let syn::NestedMeta::Meta(syn::Meta::Path(path)) = arg {
            if let Some(ident) = path.get_ident() {
                match ident.to_string().to_lowercase().as_str() {
                    "loaded_module" => loaded_module = true,
                    name => {
                        let msg = format!("Unknown attribute {} is specified; expected `loaded_module` or none", name);
                        return syn::Error::new_spanned(path, msg).to_compile_error().into()
                    }
                }
            }
        }
    }

    if parsed_fn.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(&parsed_fn.sig.inputs, "the test function must accept exactly one argument of type redis::Connection")
            .to_compile_error()
            .into()
    }

    let name = &parsed_fn.sig.ident;
    let params = &parsed_fn.sig.inputs;
    let body = &parsed_fn.block;
    let attrs = &parsed_fn.attrs;
    let vis = &parsed_fn.vis;
    let ret = &parsed_fn.sig.output;

    let conn_param = match params.first() {
        Some(FnArg::Typed(PatType { pat, .. })) => {
            match pat.deref() {
                syn::Pat::Ident(syn::PatIdent { ident, .. }) => ident,
                _ => panic!("Expected ident"),
            }
        }
        _ => panic!("Expected ident")
    };

    let prelude = if loaded_module {
        quote! { load_module(&mut #conn_param)?; }
    } else {
        quote! { }
    };

    (quote! {
        #[::core::prelude::v1::test]
        #(#attrs)*
        #vis fn #name() #ret {
            with_redis_conn(stringify!(#name), |#params| {
                #prelude
                #body
            })
        }
    }).into()
}
