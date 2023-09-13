use crate::shared::csv::save_csv;

use super::{
    enums::signal_category::SignalCategory, errors::Error,
    indicators::get_test_data_with_indicators, traits::signal::Signal,
};

#[test]
fn test_multiple_stochastic_with_threshold_short_signal_consistency() -> Result<(), Error> {
    let lf = get_test_data_with_indicators()?.drop_nulls(None);
    let mut df = lf.clone().drop_nulls(None).collect()?.clone();

    let windows = vec![3, 5, 15];
    let anchor_symbol = String::from("BTCUSDT");

    let signal = MultipleStochasticWithThresholdShortSignal {
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
    };

    let benchmark_lf = signal.set_signal_column(&lf)?;
    let benchmark_df = benchmark_lf.collect()?;

    let df_height = df.height();

    let binding = signal.clone().signal_category();
    let short_col = binding.get_column();
    let filled_i32_nulls_vector: Vec<Option<i32>> = vec![None; df_height];
    let short_series = Series::new(short_col, filled_i32_nulls_vector.clone());
    df.with_column(short_series)?;

    let start_time_ca: Vec<_> = df.column("start_time")?.datetime()?.into_iter().collect();
    let schema = df.schema();
    let mut update_df = DataFrame::from(&schema);

    for index in 0..df_height {
        let mask = df
            .column("start_time")?
            .equal(start_time_ca[index].unwrap())?;

        let last_row = df.filter(&mask)?;
        update_df = update_df.vstack(&last_row)?;
        update_df = signal.update_signal_column(&update_df)?;
    }

    let result_df = benchmark_df.left_join(&update_df, ["start_time"], ["start_time"])?;

    let path = "data/test".to_string();
    let file_name = "multiple_stochastic_with_threshold_short_signal_consistency.csv".to_string();
    save_csv(path.clone(), file_name, &result_df, true)?;

    Ok(())
}

#[test]
fn test_multiple_stochastic_with_threshold_long_signal_consistency() -> Result<(), Error> {
    let lf = get_test_data_with_indicators()?.drop_nulls(None);
    let mut df = lf.clone().drop_nulls(None).collect()?.clone();

    let windows = vec![3, 5, 15];
    let anchor_symbol = String::from("BTCUSDT");

    let signal = MultipleStochasticWithThresholdLongSignal {
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
    };

    let benchmark_lf = signal.set_signal_column(&lf)?;
    let benchmark_df = benchmark_lf.collect()?;

    let df_height = df.height();

    let binding = signal.clone().signal_category();
    let long_col = binding.get_column();
    let filled_i32_nulls_vector: Vec<Option<i32>> = vec![None; df_height];
    let long_series = Series::new(long_col, filled_i32_nulls_vector.clone());
    df.with_column(long_series)?;

    let start_time_ca: Vec<_> = df.column("start_time")?.datetime()?.into_iter().collect();
    let schema = df.schema();
    let mut update_df = DataFrame::from(&schema);

    for index in 0..df_height {
        let mask = df
            .column("start_time")?
            .equal(start_time_ca[index].unwrap())?;

        let last_row = df.filter(&mask)?;
        update_df = update_df.vstack(&last_row)?;
        update_df = signal.update_signal_column(&update_df)?;
    }

    let result_df = benchmark_df.left_join(&update_df, ["start_time"], ["start_time"])?;

    let path = "data/test".to_string();
    let file_name = "multiple_stochastic_with_threshold_long_signal_consistency.csv".to_string();
    save_csv(path.clone(), file_name, &result_df, true)?;

    Ok(())
}

#[test]
fn test_multiple_stochastic_with_threshold_close_short_signal_consistency() -> Result<(), Error> {
    let lf = get_test_data_with_indicators()?.drop_nulls(None);
    let mut df = lf.clone().drop_nulls(None).collect()?.clone();

    let windows = vec![3, 5, 15];
    let anchor_symbol = String::from("BTCUSDT");
    let upper_threshold = 70;
    let lower_threshold = 30;
    let close_window_index = 2;

    let signal = MultipleStochasticWithThresholdCloseShortSignal {
        anchor_symbol: anchor_symbol.clone(),
        windows: windows.clone(),
        upper_threshold,
        lower_threshold,
        close_window_index,
    };

    let benchmark_lf = signal.set_signal_column(&lf)?;
    let benchmark_df = benchmark_lf.collect()?;

    let df_height = df.height();

    let binding = signal.clone().signal_category();
    let short_close_col = binding.get_column();
    let filled_i32_nulls_vector: Vec<Option<i32>> = vec![None; df_height];
    let short_close_series = Series::new(short_close_col, filled_i32_nulls_vector.clone());
    df.with_column(short_close_series)?;

    let start_time_ca: Vec<_> = df.column("start_time")?.datetime()?.into_iter().collect();
    let schema = df.schema();
    let mut update_df = DataFrame::from(&schema);

    for index in 0..df_height {
        let mask = df
            .column("start_time")?
            .equal(start_time_ca[index].unwrap())?;

        let last_row = df.filter(&mask)?;
        update_df = update_df.vstack(&last_row)?;
        update_df = signal.update_signal_column(&update_df)?;
    }

    let result_df = benchmark_df.left_join(&update_df, ["start_time"], ["start_time"])?;

    let path = "data/test".to_string();
    let file_name =
        "multiple_stochastic_with_threshold_close_short_signal_consistency.csv".to_string();
    save_csv(path.clone(), file_name, &result_df, true)?;

    Ok(())
}

#[test]
fn test_multiple_stochastic_with_threshold_close_long_signal_consistency() -> Result<(), Error> {
    let lf = get_test_data_with_indicators()?.drop_nulls(None);
    let mut df = lf.clone().drop_nulls(None).collect()?.clone();

    let windows = vec![3, 5, 15];
    let anchor_symbol = String::from("BTCUSDT");
    let upper_threshold = 70;
    let lower_threshold = 30;
    let close_window_index = 2;

    let signal = MultipleStochasticWithThresholdCloseLongSignal {
        anchor_symbol: anchor_symbol.clone(),
        windows: windows.clone(),
        upper_threshold,
        lower_threshold,
        close_window_index,
    };

    let benchmark_lf = signal.set_signal_column(&lf)?;
    let benchmark_df = benchmark_lf.collect()?;

    let df_height = df.height();

    let binding = signal.clone().signal_category();
    let long_close_col = binding.get_column();
    let filled_i32_nulls_vector: Vec<Option<i32>> = vec![None; df_height];
    let long_close_series = Series::new(long_close_col, filled_i32_nulls_vector.clone());
    df.with_column(long_close_series)?;

    let start_time_ca: Vec<_> = df.column("start_time")?.datetime()?.into_iter().collect();
    let schema = df.schema();
    let mut update_df = DataFrame::from(&schema);

    for index in 0..df_height {
        let mask = df
            .column("start_time")?
            .equal(start_time_ca[index].unwrap())?;

        let last_row = df.filter(&mask)?;
        update_df = update_df.vstack(&last_row)?;
        update_df = signal.update_signal_column(&update_df)?;
    }

    let result_df = benchmark_df.left_join(&update_df, ["start_time"], ["start_time"])?;

    let path = "data/test".to_string();
    let file_name =
        "multiple_stochastic_with_threshold_close_long_signal_consistency.csv".to_string();
    save_csv(path.clone(), file_name, &result_df, true)?;

    Ok(())
}
