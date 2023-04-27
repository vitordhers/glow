use strum_macros::{Display, EnumString};
use serde::{Deserialize, Serialize};


#[derive(EnumString, Serialize, Deserialize, Debug, Display)]
pub enum RequestMethod {
    #[strum(serialize = "SUBSCRIBE")]
    SUBSCRIBE,
    #[strum(serialize = "UNSUBSCRIBE")]
    UNSUBSCRIBE,
    #[strum(serialize = "LIST_SUBSCRIPTIONS")]
    LISTSUBSCRIPTIONS,
    #[strum(serialize = "SET_PROPERTY")]
    SETPROPERTY,
    #[strum(serialize = "GET_PROPERTY")]
    GETPROPERTY,
}
