use crossterm::{
    event::{poll, read, Event, KeyCode, KeyEvent},
    terminal::{disable_raw_mode, enable_raw_mode},
    ExecutableCommand,
};
use dialoguer::{theme::ColorfulTheme, Select};
use std::{
    io::{self, Write},
    time::Duration,
};

pub fn select_from_list<T: ToString>(prompt: &str, items: &[T], default: Option<usize>) -> usize {
    Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&items)
        .default(default.unwrap_or_default())
        .interact()
        .expect("Failed to read input")
}

pub fn get_escapable_input(current_value: Option<String>) -> Option<String> {
    enable_raw_mode().unwrap();
    let polling_duration = Duration::from_millis(500);

    let mut input = current_value.unwrap_or_default();

    let result = loop {
        if poll(polling_duration).unwrap() {
            if let Event::Key(KeyEvent { code, .. }) = read().unwrap() {
                match code {
                    KeyCode::Esc => {
                        // println!("ESC key pressed, exiting...");
                        break None;
                    }
                    KeyCode::Enter => {
                        break Some(input);
                    }
                    KeyCode::Char(c) => {
                        input.push(c);
                    }
                    KeyCode::Backspace => {
                        input.pop();
                        // print!("\x08 \x08"); // Move cursor back, print space, move cursor back again
                    }
                    _ => {}
                }
            }
        }
    };

    disable_raw_mode().unwrap();

    result
}
