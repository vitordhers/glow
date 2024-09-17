use crossterm::{
    cursor::{position, MoveTo},
    event::{read, Event, KeyCode, KeyEvent},
    execute,
    style::Print,
    terminal::{disable_raw_mode, enable_raw_mode, Clear, ClearType},
};
use dialoguer::{theme::ColorfulTheme, Select};
use std::io::stdout;

pub fn select_from_list<T: ToString>(prompt: &str, items: &[T], default: Option<usize>) -> usize {
    Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&items)
        .default(default.unwrap_or_default())
        .interact()
        .expect("Failed to read input")
}

pub fn get_escapable_input(
    current_value: Option<String>,
    max_len: Option<usize>,
) -> Option<String> {
    enable_raw_mode().unwrap();
    let mut input = current_value.unwrap_or_default();

    let initial_cursor_position = position().unwrap();

    let result = loop {
        execute!(
            stdout(),
            Clear(ClearType::CurrentLine),
            MoveTo(initial_cursor_position.0, initial_cursor_position.1),
            Print(format!("{}", input)),
            MoveTo(input.len() as u16, initial_cursor_position.1),
        )
        .expect("Failed to clear the terminal");

        if let Event::Key(KeyEvent { code, .. }) = read().unwrap() {
            // println!("KEY CODE {:?}", code);
            match code {
                KeyCode::Esc => {
                    // println!("ESC key pressed, exiting...");
                    break None;
                }
                KeyCode::Enter => {
                    break Some(input);
                }
                KeyCode::Char(c) => {
                    if let Some(max_len) = max_len {
                        if input.len() < max_len {
                            input.push(c);
                        }
                    } else {
                        input.push(c);
                    }
                }
                KeyCode::Backspace => {
                    input.pop();
                    // print!("\x08 \x08"); // Move cursor back, print space, move cursor back again
                }
                _ => {}
            }
        }
    };

    disable_raw_mode().unwrap();

    result
}
