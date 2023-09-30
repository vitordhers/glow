fn set_indicator_columns(&self, lf: LazyFrame) -> Result<LazyFrame, Error> {
    let mut resampled_lfs = vec![];

    let lf_full_mins = lf.clone().select([col("start_time")]);

    // let resampled_data = get_resampled_ohlc_window_data(&lf, &self.anchor_symbol, window)?;
    // let (_, high_col, close_col, low_col) =
    //     get_symbol_window_ohlc_cols(&self.anchor_symbol, &window.to_string());
    let (_, high_col, low_col, close_col) = get_symbol_ohlc_cols(&self.anchor_symbol);

    for window in &self.windows {
        let suffix = format!("{}_{}", self.anchor_symbol, window);

        let k_column = format!("K%_{}", suffix);
        let d_column = format!("D%_{}", suffix);

        let k_window = 14;
        let d_window = 3;

        let rolling_k_opts = RollingOptions {
            window_size: Duration::parse(&format!("{}i", k_window)),
            min_periods: k_window as usize,
            center: false,
            by: Some("start_time".to_string()),
            weights: None,
            closed_window: Some(ClosedWindow::Right),
            fn_params: None,
        };

        let rolling_mean_d_opts = RollingOptions {
            window_size: Duration::parse(&format!("{}i", d_window)),
            min_periods: d_window as usize,
            center: false,
            by: Some("start_time".to_string()),
            weights: None,
            closed_window: Some(ClosedWindow::Right),
            fn_params: None,
        };

        let resampled_lf = lf
            .clone()
            .with_column(
                ((lit(100)
                    * (col(&close_col) - col(&low_col).rolling_min(rolling_k_opts.clone()))
                    / (col(&high_col).rolling_max(rolling_k_opts.clone())
                        - col(&low_col).rolling_min(rolling_k_opts)))
                .round(2))
                .alias(&k_column),
            )
            // TODO: define forward fill with windows
            // .with_column(col(&k_column).forward_fill(None).keep_name())
            .with_column(
                (col(&k_column).rolling_mean(rolling_mean_d_opts).round(2)).alias(&d_column),
            )
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
            .select(vec![col("start_time"), col(&k_column), col(&d_column)]);

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

        // // let resampled_lf_min = forward_fill_lf(resampled_lf_with_full_mins, window, 1)?;

        // // TODO: implement with with_columns_seq
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
