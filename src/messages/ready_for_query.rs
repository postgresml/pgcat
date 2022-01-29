
#[derive(PartialEq, Copy, Clone)]
pub enum TransactionStatusIndicator {
    Idle,
    TransactionBlock,
    FailedTransactionBlock,
}

impl std::convert::Into<u8> for TransactionStatusIndicator {
    fn into(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::TransactionBlock => b'T',
            Self::FailedTransactionBlock => b'E',
        }
    }
}

pub struct ReadyForQuery {
    state: TransactionStatusIndicator,
}

impl std::convert::From<&ReadyForQuery> for Vec<u8> {
    fn from(ready_for_query: &ReadyForQuery) -> Vec<u8> {
        let indicator = match ready_for_query.state {
            TransactionStatusIndicator::Idle => b'I',
            TransactionStatusIndicator::TransactionBlock => b'T',
            TransactionStatusIndicator::FailedTransactionBlock => b'E',
        };

        // Z
        let mut res = vec![b'Z'];

        // Length: 5
        res.extend(&5i32.to_be_bytes());

        // State
        res.push(indicator);

        res
    }
}

impl std::convert::Into<Vec<u8>> for ReadyForQuery {
    fn into(self) -> Vec<u8> {
        Vec::<u8>::from(&self)
    }
}

impl ReadyForQuery {
    pub fn new(state: TransactionStatusIndicator) -> Self {
        ReadyForQuery {
            state: state,
        }
    }
}