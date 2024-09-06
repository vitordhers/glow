use chrono::Duration;
use std::fmt::{Display, Formatter, Result};

#[derive(Clone, Debug)]
pub struct Statistics {
    success_rate: f64,
    current_balance: f64,
    risk: f64,
    downside_deviation: f64,
    risk_adjusted_return: f64,
    max_drawdown: f64,
    max_drawdown_duration: Duration,
    sharpe_ratio: f64,
    sortino_ratio: f64,
    calmar_ratio: f64,
}

impl Display for Statistics {
    fn fmt(&self, f: &mut Formatter) -> Result {
        write!(
            f,
            r#"
🏆 Success rate (%): {:.2}
💰 Last balance (USDT): {:.4}
📊 Risk: {:.6}
📐 Downside deviation: {:.6}
📏 Risk adjusted return: {:.6}
📉 Max drawndown (%): {:.4}
⏳ Max drawdown duration: {}h{}
📝 Sharpe: {:.2}
📝 Sortino: {:.2}
📝 Calmar: {:.2}"#,
            self.success_rate,
            self.current_balance,
            self.risk,
            self.downside_deviation,
            self.risk_adjusted_return,
            self.max_drawdown,
            self.max_drawdown_duration.num_hours(),
            self.max_drawdown_duration.num_minutes() % 60,
            self.sharpe_ratio,
            self.sortino_ratio,
            self.calmar_ratio
        )
    }
}

impl Statistics {
    pub fn new(
        success_rate: f64,
        current_balance: f64,
        risk: f64,
        downside_deviation: f64,
        risk_adjusted_return: f64,
        max_drawdown: f64,
        max_drawdown_duration: Duration,
        sharpe_ratio: f64,
        sortino_ratio: f64,
        calmar_ratio: f64,
    ) -> Self {
        Statistics {
            success_rate,
            current_balance,
            risk,
            downside_deviation,
            risk_adjusted_return,
            max_drawdown,
            max_drawdown_duration,
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
        }
    }
}

impl Default for Statistics {
    fn default() -> Self {
        Self {
            success_rate: 0.0,
            current_balance: 0.0,
            risk: 0.0,
            downside_deviation: 0.0,
            risk_adjusted_return: 0.0,
            max_drawdown: 0.0,
            max_drawdown_duration: Duration::minutes(0),
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            calmar_ratio: 0.0,
        }
    }
}
