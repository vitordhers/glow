use glow_error::GlowError;

pub fn calculate_span_alpha(span: f64) -> Result<f64, GlowError> {
    if span < 1.0 {
        panic!("Require 'span' >= 1 (found {})", span);
    }
    Ok(2.0 / (span + 1.0))
}

pub fn get_calculation_minutes(initial_minute: u32, window: &u32) -> Vec<u32> {
    let mut minutes = vec![initial_minute];

    let mut forward_minute = initial_minute;
    while forward_minute < 60 {
        forward_minute += window;
        if forward_minute < 60 {
            minutes.push(forward_minute);
        }
    }

    let mut backward_minute = initial_minute;
    while backward_minute > 0 {
        match backward_minute.checked_sub(*window) {
            Some(result) => {
                backward_minute = result;
                minutes.push(backward_minute);
            }
            None => {
                backward_minute = 0;
            }
        }
    }

    minutes.sort();

    minutes
}

// TODO: deprecate this
// pub fn get_strategy(selected_strategy: StrategyId) -> Strategy {
//     let result = STRATEGIES_MAP.get(&selected_strategy).expect(&format!(
//         "Strategy {:?} to exist at STRATEGIES_MAP",
//         selected_strategy
//     ));

//     result.clone()
// }
