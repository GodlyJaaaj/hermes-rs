use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Lit, Meta, Result};

/// Extract custom subject from `#[event(subject = "...")]`
fn extract_custom_subject(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if !attr.path().is_ident("event") {
            continue;
        }
        if let Ok(Meta::NameValue(nv)) = attr.parse_args::<Meta>() {
            if nv.path.is_ident("subject") {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: Lit::Str(s), ..
                }) = &nv.value
                {
                    return Some(s.value());
                }
            }
        }
    }
    None
}

pub fn expand_event(input: DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;
    let name_str = name.to_string();

    // Check for custom subject override: #[event(subject = "a.b.c")]
    if let Some(custom) = extract_custom_subject(&input.attrs) {
        return Ok(quote! {
            impl ::scylla_broker_core::Event for #name {
                fn subjects() -> ::std::vec::Vec<::scylla_broker_core::Subject> {
                    ::std::vec![::scylla_broker_core::Subject::from(#custom)]
                }
                fn subject(&self) -> ::scylla_broker_core::Subject {
                    ::scylla_broker_core::Subject::from(#custom)
                }
            }
        });
    }

    match &input.data {
        Data::Struct(_) => expand_struct_event(name, &name_str),
        Data::Enum(data) => expand_enum_event(name, &name_str, data),
        Data::Union(_) => Err(syn::Error::new_spanned(
            name,
            "Event cannot be derived for unions",
        )),
    }
}

fn expand_struct_event(name: &syn::Ident, name_str: &str) -> Result<TokenStream> {
    Ok(quote! {
        impl ::scylla_broker_core::Event for #name {
            fn subjects() -> ::std::vec::Vec<::scylla_broker_core::Subject> {
                ::std::vec![
                    ::scylla_broker_core::Subject::from_module_path(
                        ::std::module_path!(),
                        #name_str,
                    )
                ]
            }

            fn subject(&self) -> ::scylla_broker_core::Subject {
                ::scylla_broker_core::Subject::from_module_path(
                    ::std::module_path!(),
                    #name_str,
                )
            }
        }
    })
}

fn expand_enum_event(
    name: &syn::Ident,
    name_str: &str,
    data: &syn::DataEnum,
) -> Result<TokenStream> {
    let variant_subjects: Vec<_> = data
        .variants
        .iter()
        .map(|v| {
            let vname = v.ident.to_string();
            quote! {
                ::scylla_broker_core::Subject::from_module_path(
                    ::std::module_path!(),
                    #name_str,
                ).str(#vname)
            }
        })
        .collect();

    let match_arms: Vec<_> = data
        .variants
        .iter()
        .map(|v| {
            let vident = &v.ident;
            let vname = v.ident.to_string();
            let pattern = match &v.fields {
                Fields::Named(_) => quote! { Self::#vident { .. } },
                Fields::Unnamed(_) => quote! { Self::#vident(..) },
                Fields::Unit => quote! { Self::#vident },
            };
            quote! {
                #pattern => ::scylla_broker_core::Subject::from_module_path(
                    ::std::module_path!(),
                    #name_str,
                ).str(#vname)
            }
        })
        .collect();

    Ok(quote! {
        impl ::scylla_broker_core::Event for #name {
            fn subjects() -> ::std::vec::Vec<::scylla_broker_core::Subject> {
                ::std::vec![#(#variant_subjects),*]
            }

            fn subject(&self) -> ::scylla_broker_core::Subject {
                match self {
                    #(#match_arms),*
                }
            }
        }
    })
}
