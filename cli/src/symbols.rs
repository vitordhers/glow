use common::{
    enums::symbol_id::SymbolId,
    r#static::SYMBOLS_MAP,
    structs::{Symbol, SymbolsPair},
};

use crate::select_from_list;

pub fn change_symbols_pair(current_symbols_pair: SymbolsPair) -> Option<SymbolsPair> {
    let result = loop {
        let benchmark_symbols_pair_options = vec![
            "⚓ Change Anchor Symbol".to_owned(),
            "📈 Change Traded Symbol".to_owned(),
            "🔙 Go back".to_owned(),
        ];

        let back_index = benchmark_symbols_pair_options.len() - 1;
        let selection = select_from_list(
            "Select a Symbol to change",
            &benchmark_symbols_pair_options,
            Some(back_index),
        );

        break match selection {
            0 => {
                let selection = get_different_than_symbol(current_symbols_pair.anchor.id, "Anchor");
                if selection.is_none() {
                    continue;
                }
                let selected_symbol = selection.unwrap();
                let mut updated_symbols_pair = current_symbols_pair.clone();
                updated_symbols_pair.anchor = selected_symbol;
                Some(updated_symbols_pair)
            }
            1 => {
                let selection = get_different_than_symbol(current_symbols_pair.traded.id, "Traded");
                if selection.is_none() {
                    continue;
                }
                let selected_symbol = selection.unwrap();
                let mut updated_symbols_pair = current_symbols_pair.clone();
                updated_symbols_pair.anchor = selected_symbol;
                Some(updated_symbols_pair)
            }
            _ => None,
        };
    };
    result
}

fn get_different_than_symbol(symbol_id: SymbolId, symbol_type: &str) -> Option<&'static Symbol> {
    let mut filtered_symbols = SYMBOLS_MAP
        .into_iter()
        .filter_map(|(_, symbol)| {
            if symbol.id != symbol_id {
                Some(symbol.name)
            } else {
                None
            }
        })
        .collect::<Vec<&str>>();

    filtered_symbols.push("🔙 Go back");

    let back_index = filtered_symbols.len() - 1;
    let selection = select_from_list(
        &format!("Select new {} symbol", symbol_type),
        &filtered_symbols,
        Some(back_index),
    );

    match selection {
        selected_back_index if selected_back_index == back_index => None,
        selected_symbol_index => {
            let selected_name = filtered_symbols.get(selected_symbol_index).expect(&format!(
                "filtered symbols {:?} to has selected symbol index {}",
                filtered_symbols, selected_symbol_index
            ));

            let selected_symbol = SYMBOLS_MAP.get(&selected_name).expect(&format!(
                "Symbol {} to exist at symbols map {:?}",
                selected_name, SYMBOLS_MAP
            ));
            Some(&selected_symbol)
        }
    }
}
