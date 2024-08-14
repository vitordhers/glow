use crate::shared::csv::save_csv;
use chrono::{NaiveDateTime, ParseError};

fn load_test_df() -> Result<DataFrame, Error> {
    use std::env;

    let path = "data/test".to_string();

    let file_name = "tick_data.csv".to_string();
    let current_dir = env::current_dir().expect("Failed to get current directory");
    let file_path = format!("{}/{}/{}", current_dir.display(), &path, &file_name);

    let mut df = CsvReader::from_path(file_path)?
        // .with_dtypes(Some(tick_data_schema.into()))
        .has_header(true)
        .finish()?;

    let mut start_time_series = vec![];

    let df_clone = df.clone();
    let stringfied_col = df_clone.column("start_time")?.utf8()?;

    let mut iter = stringfied_col.into_iter();

    while let Some(start_time) = iter.next() {
        let datetime = parse_datetime(start_time.unwrap()).unwrap();
        start_time_series.push(datetime);
    }

    let start_time_series = Series::new("start_time", start_time_series);
    df.replace("start_time", start_time_series)?;

    Ok(df)
}

fn parse_datetime(s: &str) -> Result<NaiveDateTime, ParseError> {
    let format = "%Y-%m-%dT%H:%M:%S%.3f"; // Specify the format of your input string
    NaiveDateTime::parse_from_str(s, format)
}

#[test]
fn test_ema_indicator_consistency() -> Result<(), Error> {
    let mut df = load_test_df()?;

    let anchor_symbol = String::from("BTCUSDT");
    let trend_col = String::from("bullish_market");

    let indicator = ExponentialMovingAverageIndicator {
        name: "ExponentialMovingAverageIndicator".into(),
        anchor_symbol: anchor_symbol.clone(),
        long_span: 50,
        short_span: 21,
        trend_col: trend_col.clone(),
    };

    let benchmark_lf = indicator.set_indicator_columns(df.clone().lazy())?;

    // println!("benchmark df {:?}", benchmark_lf.collect()?);

    let df_height = df.height();

    let ema_short_col = &format!("{}_ema_s", &anchor_symbol);
    let ema_long_col = &format!("{}_ema_l", &anchor_symbol);
    let filled_f64_nulls_vector: Vec<Option<f64>> = vec![None; df_height];
    let filled_i32_nulls_vector: Vec<Option<i32>> = vec![None; df_height];
    let ema_s_series = Series::new(ema_short_col, filled_f64_nulls_vector.clone());
    df.with_column(ema_s_series)?;
    let ema_l_series = Series::new(ema_long_col, filled_f64_nulls_vector.clone());
    df.with_column(ema_l_series)?;

    let trend_col_series = Series::new(&trend_col, filled_i32_nulls_vector);
    df.with_column(trend_col_series)?;

    let start_time_ca: Vec<_> = df.column("start_time")?.datetime()?.into_iter().collect();
    let schema = df.schema();
    let mut update_df = DataFrame::from(&schema);

    for index in 0..df_height {
        let mask = df
            .column("start_time")?
            .equal(start_time_ca[index].unwrap())?;

        let last_row = df.filter(&mask)?;
        update_df = update_df.vstack(&last_row)?;
        update_df = indicator.update_indicator_columns(&update_df)?;
    }

    let benchmark_df = benchmark_lf.collect()?;

    assert_eq!(
        benchmark_df
            .column(&trend_col)?
            .i32()?
            .into_iter()
            .collect::<Vec<_>>(),
        update_df
            .column(&trend_col)?
            .i32()?
            .into_iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        benchmark_df
            .column(&ema_short_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>(),
        update_df
            .column(&ema_short_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        benchmark_df
            .column(&ema_long_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>(),
        update_df
            .column(&ema_long_col)?
            .f64()?
            .into_iter()
            .collect::<Vec<_>>(),
    );

    let result_df = benchmark_df.left_join(&update_df, ["start_time"], ["start_time"])?;

    let path = "data/test".to_string();
    let file_name = "ema_consistency.csv".to_string();
    save_csv(path.clone(), file_name, &result_df, true)?;

    Ok(())
}

#[test]
fn test_stochastic_threshold_indicator_consistency() -> Result<(), Error> {
    let mut df = load_test_df()?;

    let anchor_symbol = String::from("BTCUSDT");
    let trend_col = String::from("bullish_market");

    let pre_indicator = ExponentialMovingAverageIndicator {
        name: "ExponentialMovingAverageIndicator".into(),
        anchor_symbol: anchor_symbol.clone(),
        long_span: 50,
        short_span: 21,
        trend_col: trend_col.clone(),
    };

    let preindicator_lf = pre_indicator.set_indicator_columns(df.clone().lazy())?;

    df = df
        .lazy()
        .left_join(preindicator_lf, "start_time", "start_time")
        .collect()?;

    let upper_threshold = 70;
    let lower_threshold = 30;

    let indicator = StochasticThresholdIndicator {
        name: "StochasticThresholdIndicator".into(),
        upper_threshold,
        lower_threshold,
        trend_col: trend_col.clone(),
    };

    let benchmark_lf = indicator.set_indicator_columns(df.clone().lazy())?;

    let df_height = df.height();

    let long_threshold_col = String::from("long_threshold");
    let short_threshold_col = String::from("short_threshold");
    let filled_i32_nulls_vector: Vec<Option<i32>> = vec![None; df_height];
    let short_threshold_series = Series::new(&short_threshold_col, filled_i32_nulls_vector.clone());
    df.with_column(short_threshold_series)?;
    let long_threshold_series = Series::new(&long_threshold_col, filled_i32_nulls_vector.clone());
    df.with_column(long_threshold_series)?;

    let start_time_ca: Vec<_> = df.column("start_time")?.datetime()?.into_iter().collect();
    let schema = df.schema();
    let mut update_df = DataFrame::from(&schema);

    for index in 0..df_height {
        let mask = df
            .column("start_time")?
            .equal(start_time_ca[index].unwrap())?;

        let last_row = df.filter(&mask)?;
        update_df = update_df.vstack(&last_row)?;
        update_df = indicator.update_indicator_columns(&update_df)?;
    }

    let benchmark_df = benchmark_lf.collect()?;

    assert_eq!(
        benchmark_df
            .column(&long_threshold_col)?
            .i32()?
            .into_iter()
            .collect::<Vec<_>>(),
        update_df
            .column(&long_threshold_col)?
            .i32()?
            .into_iter()
            .collect::<Vec<_>>()
    );
    assert_eq!(
        benchmark_df
            .column(&short_threshold_col)?
            .i32()?
            .into_iter()
            .collect::<Vec<_>>(),
        update_df
            .column(&short_threshold_col)?
            .i32()?
            .into_iter()
            .collect::<Vec<_>>()
    );

    let result_df = benchmark_df.left_join(&update_df, ["start_time"], ["start_time"])?;

    let path = "data/test".to_string();
    let file_name = "stochastic_threshold_consistency.csv".to_string();
    save_csv(path.clone(), file_name, &result_df, true)?;

    Ok(())
}

#[test]
fn test_stochastic_indicator_consistency() -> Result<(), Error> {
    let mut df = load_test_df()?;
    let mut lf = df.clone().lazy();

    let anchor_symbol = String::from("BTCUSDT");
    let trend_col = String::from("bullish_market");

    let pre_indicator = ExponentialMovingAverageIndicator {
        name: "ExponentialMovingAverageIndicator".into(),
        anchor_symbol: anchor_symbol.clone(),
        long_span: 50,
        short_span: 21,
        trend_col: trend_col.clone(),
    };

    let preindicator_lf = pre_indicator.set_indicator_columns(df.clone().lazy())?;

    let upper_threshold = 70;
    let lower_threshold = 30;

    let stochastic_threshold_indicator = StochasticThresholdIndicator {
        name: "StochasticThresholdIndicator".into(),
        upper_threshold,
        lower_threshold,
        trend_col: trend_col.clone(),
    };

    let stochastic_threshold_indicator_lf =
        stochastic_threshold_indicator.set_indicator_columns(preindicator_lf)?;

    lf = lf.left_join(
        stochastic_threshold_indicator_lf,
        "start_time",
        "start_time",
    );

    df = lf.clone().collect()?;

    let windows = vec![3, 5, 15];

    let indicator = StochasticIndicator {
        name: "StochasticIndicator".into(),
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
    };

    let indicator_lf = indicator.set_indicator_columns(lf.clone())?;

    lf = lf.left_join(indicator_lf, "start_time", "start_time");

    // println!("stochastic indicator df {:?}", lf.collect()?);

    let df_height = df.height();

    let filled_f64_nulls_vector: Vec<Option<f64>> = vec![None; df_height];
    for window in windows {
        let suffix = format!("{}_{}", anchor_symbol, window);

        let k_column = format!("K%_{}", suffix);
        let d_column = format!("D%_{}", suffix);
        let k_percent_series = Series::new(&k_column, filled_f64_nulls_vector.clone());
        let d_percent_series = Series::new(&d_column, filled_f64_nulls_vector.clone());
        df.with_column(k_percent_series)?;
        df.with_column(d_percent_series)?;
    }

    let start_time_ca: Vec<_> = df.column("start_time")?.datetime()?.into_iter().collect();
    let schema = df.schema();
    let mut update_df = DataFrame::from(&schema);

    for index in 0..df_height {
        let mask = df
            .column("start_time")?
            .equal(start_time_ca[index].unwrap())?;

        let last_row = df.filter(&mask)?;
        update_df = update_df.vstack(&last_row)?;
        update_df = indicator.update_indicator_columns(&update_df)?;
    }

    let benchmark_df = lf.collect()?;

    let result_df = benchmark_df.left_join(&update_df, ["start_time"], ["start_time"])?;

    let path = "data/test".to_string();
    let file_name = "stochastic_consistency.csv".to_string();
    save_csv(path.clone(), file_name, &result_df, true)?;

    Ok(())
}

pub fn get_test_data_with_indicators() -> Result<LazyFrame, Error> {
    let mut df = load_test_df()?;
    let mut lf = df.clone().lazy();

    let anchor_symbol = String::from("BTCUSDT");
    let trend_col = String::from("bullish_market");

    let pre_indicator = ExponentialMovingAverageIndicator {
        name: "ExponentialMovingAverageIndicator".into(),
        anchor_symbol: anchor_symbol.clone(),
        long_span: 50,
        short_span: 21,
        trend_col: trend_col.clone(),
    };

    let preindicator_lf = pre_indicator.set_indicator_columns(df.clone().lazy())?;

    let upper_threshold = 70;
    let lower_threshold = 30;

    let stochastic_threshold_indicator = StochasticThresholdIndicator {
        name: "StochasticThresholdIndicator".into(),
        upper_threshold,
        lower_threshold,
        trend_col: trend_col.clone(),
    };

    let stochastic_threshold_indicator_lf =
        stochastic_threshold_indicator.set_indicator_columns(preindicator_lf)?;

    lf = lf.left_join(
        stochastic_threshold_indicator_lf,
        "start_time",
        "start_time",
    );

    df = lf.clone().collect()?;

    let windows = vec![3, 5, 15];

    let indicator = StochasticIndicator {
        name: "StochasticIndicator".into(),
        windows: windows.clone(),
        anchor_symbol: anchor_symbol.clone(),
    };

    let indicator_lf = indicator.set_indicator_columns(lf.clone())?;

    lf = lf.left_join(indicator_lf, "start_time", "start_time");

    Ok(lf)
}
