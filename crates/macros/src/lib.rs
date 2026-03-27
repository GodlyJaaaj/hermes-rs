use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod event;
mod group;

#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    event::expand_event(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}
