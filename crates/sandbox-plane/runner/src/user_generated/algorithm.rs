use stock_trek::prelude::*;

pub struct AlgorithmStub;

impl Default for AlgorithmStub {
    fn default() -> Self {
        Self
    }
}

#[register_algorithm(default)]
impl Algorithm for AlgorithmStub {
    fn name(&self) -> &str {
        "AlgorithmStub"
    }
    fn description(&self) -> &str {
        ""
    }
    fn preferences(&self) -> Preferences {
        Preferences {
            cex: CexPreferences {
                max_network_delay_millis: 0,
                rounding: CexRoundingPreferences {
                    activation_price_triggered_above: RoundingStrategy::ToZero,
                    activation_price_triggered_below: RoundingStrategy::ToZero,
                    price: RoundingStrategy::ToZero,
                    quantity: RoundingStrategy::ToZero,
                    callback_rate_bps: RoundingStrategy::ToZero,
                },
            },
        }
    }
    fn signals(&self, _c: &SignalContext) -> Signals {
        Signals::new()
    }
    fn strategy(&self, c: &StrategyContext) -> Command {
        c.commands.no_op()
    }
}
