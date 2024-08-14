use dialoguer::{theme::ColorfulTheme, Select};

pub fn select_from_list<T: ToString>(prompt: &str, items: &[T], default: Option<usize>) -> usize {
    Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&items)
        .default(default.unwrap_or_default())
        .interact()
        .expect("Failed to read input")
}
