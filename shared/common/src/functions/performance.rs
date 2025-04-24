use super::round_down_nth_decimal;
use chrono::Duration;
use glow_error::GlowError;
use polars::prelude::*;

pub fn calculate_success_rate(returns_series: &Column) -> Result<f64, GlowError> {
    let ca = returns_series.f64()?;
    let positive_count = ca.into_no_null_iter().filter(|&x| x > 0.0).count();
    let total_count = ca.into_no_null_iter().len();
    let success_rate = if total_count != 0 {
        positive_count as f64 / total_count as f64
    } else {
        0.0 as f64
    };

    Ok(round_down_nth_decimal(success_rate, 2))
}

pub fn calculate_risk_adjusted_returns(
    returns_series: &Column,
    risk_series: &Column,
) -> Result<f64, GlowError> {
    let returns_ca = returns_series.f64()?;
    let risk_ca = risk_series.f64()?;

    let risk_adjusted_returns = returns_ca
        .into_no_null_iter()
        .enumerate()
        .zip(risk_ca.into_no_null_iter())
        .fold(0.0, |acc, ((_index, returns), risk)| {
            let iteration = if risk != 0.0 { returns / risk } else { 0.0 };
            acc + iteration
        });
    Ok(risk_adjusted_returns)
}

pub fn calculate_sharpe_ratio(
    returns_series: &Column,
    risk_series: &Column,
    risk_free_returns: f64,
) -> Result<f64, GlowError> {
    let returns_ca = returns_series.f64()?;
    let risk_ca = risk_series.f64()?;

    let risk_adjusted_returns = returns_ca
        .into_no_null_iter()
        .zip(risk_ca.into_no_null_iter())
        .fold(-risk_free_returns, |acc, (returns, risk)| {
            let iteration = if risk != 0.0 { returns / risk } else { 0.0 };
            acc + iteration
        });
    Ok(risk_adjusted_returns)
}

pub fn calculate_sortino_ratio(
    returns_series: &Column,
    downside_risk_series: &Column,
    risk_free_returns: f64,
) -> Result<f64, GlowError> {
    let returns_ca = returns_series.f64()?;
    let downside_risk_ca = downside_risk_series.f64()?;

    let risk_adjusted_returns = returns_ca
        .into_no_null_iter()
        .zip(downside_risk_ca.into_no_null_iter())
        .fold(-risk_free_returns, |acc, (returns, risk)| {
            let iteration = if risk != 0.0 { returns / risk } else { 0.0 };
            acc + iteration
        });
    Ok(risk_adjusted_returns)
}

pub fn calculate_calmar_ratio(
    balance_series: &Column,
    max_drawdown: f64,
) -> Result<f64, GlowError> {
    if max_drawdown == 0.0 {
        Ok(0.0)
    } else {
        let balances_vec: Vec<f64> = balance_series.f64()?.into_no_null_iter().collect();
        let last_balance_index = balances_vec.len() - 1;
        let last_balance = balances_vec[last_balance_index];
        Ok(last_balance / max_drawdown)
    }
}

pub fn calculate_max_drawdown_and_duration(
    start_series: &Column,
    end_series: &Column,
    drawdown_series: &Column,
) -> Result<(f64, Duration), GlowError> {
    let start_vec = start_series
        .datetime()?
        .into_no_null_iter()
        .collect::<Vec<_>>();
    let end_vec = end_series
        .datetime()?
        .into_no_null_iter()
        .collect::<Vec<_>>();
    let drawdown_vec = drawdown_series
        .f64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<f64>>();
    if let Some((max_index, _)) = drawdown_vec
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
    {
        let max_drawdown = drawdown_vec[max_index];
        let max_drawdown_duration =
            Duration::milliseconds(end_vec[max_index] - start_vec[max_index]);

        Ok((max_drawdown, max_drawdown_duration))
    } else {
        Ok((0.0, Duration::minutes(0)))
    }
}
