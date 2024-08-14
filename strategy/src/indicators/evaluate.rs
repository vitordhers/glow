fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
    let mut resampled_lfs = vec![];

    let lf_full_mins = lf.clone().select([col("start_time")]);

    let (_, high_col, low_col, close_col) = get_symbol_ohlc_cols(&self.anchor_symbol);

    let initial_minute = self.starting_datetime.minute();

    for window in &self.windows {
        let suffix = format!("{}_{}", self.anchor_symbol, window);

        let k_column = format!("K%_{}", suffix);
        let k_column_raw = format!("K%_{}_raw", suffix);
        let d_column = format!("D%_{}", suffix);

        let k_window = 14 * window;
        let smooth_k_window = 4 * window;
        let d_window = 5 * window;

        let rolling_k_opts = RollingOptions {
            window_size: Duration::parse(&format!("{}m", k_window)),
            min_periods: k_window as usize,
            center: false,
            by: Some("start_time".to_string()),
            weights: None,
            closed_window: Some(ClosedWindow::Right), // this makes sure that the current start_time is included in the analysis
            fn_params: None,
        };

        let smooth_k_opts = RollingOptions {
            window_size: Duration::parse(&format!("{}m", smooth_k_window)),
            min_periods: smooth_k_window as usize,
            center: false,
            by: Some("start_time".to_string()),
            weights: None,
            closed_window: Some(ClosedWindow::Right),
            fn_params: None,
        };

        let rolling_mean_d_opts = RollingOptions {
            window_size: Duration::parse(&format!("{}m", d_window)),
            min_periods: d_window as usize,
            center: false,
            by: Some("start_time".to_string()),
            weights: None,
            closed_window: Some(ClosedWindow::Right),
            fn_params: None,
        };

        let low_window = &format!("min_{}_{}", &low_col, &k_window);
        let high_window = &format!("max_{}_{}", &high_col, &k_window);

        let returns_output: SpecialEq<Arc<dyn FunctionOutputField>> =
            GetOutput::from_type(DataType::Boolean);

        let calculation_minutes = get_calculation_minutes(initial_minute, window);
        let window_clone = window.clone();

        let mut resampled_lf = lf
            .clone()
            .with_columns([
                col(&low_col)
                    .rolling_min(rolling_k_opts.clone())
                    .alias(low_window),
                col(&high_col)
                    .rolling_max(rolling_k_opts.clone())
                    .alias(high_window),
            ])
            .with_column(
                col("start_time")
                    .map(
                        move |start_times| {
                            let mut result_vec = vec![];
                            let datetimes = start_times.datetime().unwrap().to_vec();
                            for i in datetimes {
                                let timestamp = i.unwrap();
                                let datetime =
                                    NaiveDateTime::from_timestamp_millis(timestamp).unwrap();
                                let should_calculate =
                                    calculation_minutes.contains(&datetime.minute());
                                result_vec.push(should_calculate);
                            }

                            let series =
                                Series::new(&format!("evaluate_{}", window_clone), result_vec);

                            Ok(Some(series))
                        },
                        returns_output,
                    )
                    .alias(&format!("evaluate_{}", window_clone)),
            )
            .with_column(
                ((lit(100) * (col(&close_col) - col(low_window))
                    / (col(high_window) - col(low_window)))
                .round(2))
                .alias(&k_column_raw),
            )
            .with_column(
                when(col(&format!("evaluate_{}", window_clone)))
                    .then(
                        col(&k_column_raw)
                            .rolling_mean(smooth_k_opts.clone())
                            .round(2),
                    )
                    .otherwise(lit(NULL))
                    .alias(&k_column),
            )
            // TODO: define forward fill with windows
            .with_column(col(&k_column).forward_fill(Some(*window)).keep_name())
            // .with_column(col(&k_column).fill_null(lit(-1)))
            .with_column(
                when(col(&format!("evaluate_{}", window_clone)))
                    .then(col(&k_column).rolling_mean(rolling_mean_d_opts).round(2))
                    .otherwise(lit(NULL))
                    .alias(&d_column),
            )
            .with_column(col(&d_column).forward_fill(Some(*window)).keep_name());
        // .with_columns([
        //     when(col("window_count").lt(lit(*window)))
        //         .then(col(&k_column).shift(1))
        //         .otherwise(col(&k_column))
        //         .alias(&k_column),
        //     when(col("window_count").lt(lit(*window)))
        //         .then(col(&d_column).shift(1))
        //         .otherwise(col(&d_column))
        //         .alias(&d_column),
        // ])

        resampled_lf = resampled_lf.select(vec![col("start_time"), col(&k_column), col(&d_column)]);

        // let resampled_lf_with_full_mins = lf_full_mins
        //     .clone()
        //     .left_join(resampled_lf, "start_time", "start_time")
        //     .sort(
        //         "start_time",
        //         SortOptions {
        //             descending: false,
        //             nulls_last: false,
        //             multithreaded: true,
        //             maintain_order: true,
        //         },
        //     );

        // let resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

        // TODO: implement with with_columns_seq
        // let resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

        resampled_lfs.push(resampled_lf);
    }
    let mut new_lf = lf_full_mins.clone();

    for resampled_lf in resampled_lfs {
        new_lf = new_lf.left_join(resampled_lf, "start_time", "start_time");
    }
    let mut new_df = new_lf.collect()?;

    new_df = new_df.fill_null(FillNullStrategy::Forward(None))?;
    new_lf = new_df.lazy();

    Ok(new_lf)
}
