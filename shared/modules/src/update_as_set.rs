fn set_indicators_data(
    &self,
    data: &LazyFrame,
    initial_last_bar: NaiveDateTime,
) -> Result<LazyFrame, Error> {
    let mut data = data.to_owned();
    // let tick_data = data.clone();

    for indicator in &self.indicators {
        let lf = indicator.set_indicator_columns(data.clone())?;
        data = data.left_join(lf, "start_time", "start_time");
    }

    // first, set indicators columns of offset period
    let one_day_duration = ChronoDuration::hours(24);
    let offset_end = initial_last_bar - one_day_duration;

    // println!("@@ offset_end {:?}", offset_end);
    let offset_end_ts = offset_end.timestamp_millis();

    let offset_lf = data
        .clone()
        .filter(col("start_time").lt_eq(lit(offset_end_ts)));

    // then, iterate through every df row to set indicators data as update would
    let mut updated_df = offset_lf.collect()?;

    // println!("@@@ updated_df {:?}", updated_df);

    let data_to_update_df = data
        .filter(col("start_time").gt(lit(offset_end_ts)))
        .collect()?;

    let data_to_update_df_time_starts =
        data_to_update_df.column("start_time")?.datetime()?.to_vec();

    for indicator in &self.indicators {
        for start_time in &data_to_update_df_time_starts {
            let filter_mask = data_to_update_df
                .column("start_time")?
                .equal(start_time.unwrap())?;
            let latest_kline_df = data_to_update_df.filter(&filter_mask)?;

            updated_df = updated_df.vstack(&latest_kline_df)?;
            updated_df = indicator.update_indicator_columns(&updated_df)?;
        }
    }

    // data = data_to_update_df.lazy();
    data = updated_df.lazy();

    Ok(data)
}