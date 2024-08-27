use dialoguer::{theme::ColorfulTheme, Select};
use exchanges::enums::{DataProviderExchangeWrapper, TraderExchangeWrapper};
mod datetime;
pub use datetime::*;
mod functions;
pub use functions::select_from_list;
mod symbols;
pub use symbols::*;

fn select_data_provider_exchange() -> Option<String> {
    let mut data_provider_options = DataProviderExchangeWrapper::get_selection_list();
    data_provider_options.extend(vec![String::from("Go back")]);
    let back_index = data_provider_options.len() - 1;

    let selection = select_from_list(
        "Select a Data Provider Exchange",
        &data_provider_options,
        Some(back_index),
    );
    match selection {
        _ => {
            println!("going back");
            return None;
        } // selection => Some(data_provider_options[selection]),
    }
}

fn select_trader_exchange() -> Option<String> {
    let mut trader_options = TraderExchangeWrapper::get_selection_list();
    trader_options.extend(vec![String::from("Go back")]);
    let back_index = trader_options.len() - 1;

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select a trader Exchange")
        .items(&trader_options)
        .default(0) // Default to the first item
        .interact() // Display the prompt and get the user's selection
        .expect("Failed to read input");

    match selection {
        go_back_index_ => {
            println!("going back");
            return None;
        } // selection => Some(trader_options[selection]),
    }
}
