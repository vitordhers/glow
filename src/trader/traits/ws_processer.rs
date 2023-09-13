use crate::trader::enums::processer_action::ProcesserAction;

pub trait WsProcesser {
    fn process_ws_message(&self, json: &String) -> ProcesserAction;
}
