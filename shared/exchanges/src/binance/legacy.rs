async fn listen_ticks(
    &mut self,
    mut wss: WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<(), GlowError> {
    self.subscribe_to_tick_stream(&mut wss).await?;

    let mut current_staged_kline_start = self.benchmark_end;
    let discard_ticks_before = self.benchmark_end - ChronoDuration::nanoseconds(1);
    let mut current_staged_kline_minute = current_staged_kline_start.time().minute();

    let kline_duration_in_secs = self.kline_duration.as_secs() as i64;
    let chrono_kline_duration = ChronoDuration::from_std(self.kline_duration)
        .expect("init -> error converting std duration to chrono duration");

    // let mut staging_ticks: HashMap<u32, Vec<TickData>> = HashMap::new();
    // let ticks_data_to_process: BehaviorSubject<Vec<TickData>> = BehaviorSubject::new(vec![]);
    let mut ticks_to_commit_subscription = self.ticks_to_commit.subscribe();

    // TODO: decouple initial fetch and error handling (if convenient) in separate threads
    let start_datetime = current_datetime();
    let start_timestamp = start_datetime.timestamp();
    let seconds_until_next_minute = 60 - (start_timestamp % 60);
    let timeout_until_last_benchmark_kline_available =
        Duration::from_secs(seconds_until_next_minute as u64);
    let mut last_benchmark_kline_available_at =
        Instant::now() + timeout_until_last_benchmark_kline_available;
    let mut timeout_executed = false;
    let unique_symbols_len = self.unique_symbols.len();

    loop {
        select! {
            message_result = wss.try_next() => {
                // TODO: clean this code
                match message_result {
                    Ok(message_opt) => {
                        if let Some(message) = message_opt {
                            match message {
                                Message::Text(json) => {
                                    let response: BinanceWsResponse =
                                        serde_json::from_str::<BinanceWsResponse>(&json).unwrap_or_default();

                                    match response {
                                        BinanceWsResponse::Tick(tick) => {
                                            let tick_data: TickData = from_tick_to_tick_data(tick, &self.symbols);

                                            let tick_time = tick_data.start_time.time();
                                            let tick_minute = tick_time.minute();
                                            let tick_second = tick_time.second();
                                            // we assume that if the received tick minute is the same as the current staged kline
                                            // the tick still belongs to the kline
                                            if tick_minute == current_staged_kline_minute {
                                                self.staged_ticks.entry(tick_second).or_insert(Vec::new()).push(tick_data.clone());
                                            } else {
                                                // otherwise, all ticks regarding the staged kline were already provided
                                                // and the ticks must be committed as kline data

                                                // commit ticks to kline data
                                                self.ticks_to_commit.next(self.staged_ticks.values().cloned().into_iter().flat_map(|vec| vec.into_iter()).collect());

                                                // clear staged ticks
                                                self.staged_ticks.clear();

                                                // insert the new tick data at respective map second
                                                self.staged_ticks.insert(tick_second, vec![tick_data.clone()]);
                                                // and update current committed kline minute
                                                current_staged_kline_minute = tick_minute;
                                            }

                                            let second_staged_ticks = self.staged_ticks.get(&tick_second).unwrap();
                                            if second_staged_ticks.len() == unique_symbols_len {
                                                print!("{}", LogKlines(second_staged_ticks.to_vec()));
                                            }
                                        },
                                        _ => {}
                                    }
                                },
                                Message::Ping(_) => {
                                   wss.send(Message::Pong(vec![])).await?
                                },
                                _ => {}
                            }

                        }
                    },
                    Err(error) => {
                        let mut last_error_guard = self.last_ws_error_ts
                        .lock()
                        .expect("handle_websocket -> last_error_guard unwrap");
                        let error_timestamp = current_timestamp();
                        *last_error_guard = Some(error_timestamp);
                        eprintln!("WebSocket message error: {:?}", error);
                    }
                }
            },
            committed_ticks = ticks_to_commit_subscription.next() => {
                if committed_ticks.is_none() {
                    continue;
                }

                let mut committed_ticks = committed_ticks.unwrap();

                if committed_ticks.len() <= 0 || committed_ticks.iter().filter(|tick| tick.start_time > discard_ticks_before).collect::<Vec<_>>().len() <= 0 {
                    continue;
                }

                committed_ticks.sort_by(|a, b| a.start_time.cmp(&b.start_time));

                let prev_kline_start = current_staged_kline_start;
                current_staged_kline_start += chrono_kline_duration;


                let committed_tick_df = map_tick_data_to_df(
                    &self.trading_data_schema,
                    chrono_kline_duration,
                    prev_kline_start,
                    &committed_ticks,
                    &self.unique_symbols,
                )?;


                let trading_data_update = TradingDataUpdate::MarketData{ last_period_tick_data: committed_tick_df};
                self.trading_data_update_listener.next(trading_data_update);

            },
            // TODO: separate this if convenient
            _ = sleep_until(last_benchmark_kline_available_at) => {
                last_benchmark_kline_available_at = Instant::now() + Duration::from_secs(SECONDS_IN_DAY as u64);
                if timeout_executed {
                    continue;
                }

                let mut tick_data = Vec::new();

                let remaining_seconds_from_previous_minute = start_timestamp % 60;
                let mut start_ms = (start_timestamp - remaining_seconds_from_previous_minute) * 1000;
                let end_ms = start_ms + (kline_duration_in_secs * 1000);
                {
                    let last_error_guard = self.last_ws_error_ts.lock().expect("handle_websocket -> last_error_guard unwrap");
                    if let Some(last_error_ts) = &*last_error_guard {
                        let remainder_seconds_to_next_minute = last_error_ts % 60;
                        start_ms = (last_error_ts - remainder_seconds_to_next_minute) * 1000;
                    }
                }

                let current_limit = (end_ms - start_ms) / (kline_duration_in_secs * 1000);


                for symbol in &self.unique_symbols {
                    tick_data = Self::fetch_data(&self.http,symbol, start_ms, end_ms, current_limit).await?;
                }
                let new_ticks_data =
                    consolidate_complete_tick_data_into_lf(&self.unique_symbols, &tick_data, self.trading_data_schema)?.collect()?;

                let trading_data_update = TradingDataUpdate::MarketData{ last_period_tick_data: new_ticks_data};
                self.trading_data_update_listener.next(trading_data_update);
                {
                    let mut last_error_guard = self.last_ws_error_ts.lock().unwrap();
                    *last_error_guard = None;
                }

                timeout_executed = true
            },
        }
    }
}
