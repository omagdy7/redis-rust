use crate::commands::RedisCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Idle,      // before MULTI
    Queuing,   // after MULTI, before EXEC/DISCARD
    Discarded, // after DISCARD
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub commands: Vec<RedisCommand>,
    pub state: TxState,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            commands: Vec::new(),
            state: TxState::Idle,
        }
    }

    pub fn state(&self) -> TxState {
        self.state
    }

    pub fn multi() -> Self {
        Transaction {
            commands: Vec::new(),
            state: TxState::Queuing,
        }
    }

    pub fn clear(&mut self) {
        self.state = TxState::Idle;
        self.commands.clear();
    }

    pub fn is_empty_queue(&self) -> bool {
        return self.commands.is_empty();
    }

    pub fn queue(&mut self, cmd: RedisCommand) {
        if self.state == TxState::Queuing {
            self.commands.push(cmd);
        }
    }

    pub fn discard(&mut self) {
        self.commands.clear();
        self.state = TxState::Discarded;
    }
}
