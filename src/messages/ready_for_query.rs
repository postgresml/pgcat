use bytes::Buf;

#[derive(PartialEq, Copy, Clone, Debug)]
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
        ReadyForQuery { state: state }
    }
}

impl crate::messages::Message for ReadyForQuery {
    fn len(&self) -> i32 {
        5
    }

    fn parse(buf: &mut bytes::BytesMut, len: i32) -> Option<ReadyForQuery> {
        // 'Z': 1 byte
        // Len: 4 bytes
        let _c = buf.get_u8();
        let _len = buf.get_i32();

        let transaction_indicator = match buf.get_u8() as char {
            'I' => TransactionStatusIndicator::Idle,
            'T' => TransactionStatusIndicator::TransactionBlock,
            'E' => TransactionStatusIndicator::FailedTransactionBlock,
            _ => return None,
        };

        Some(ReadyForQuery {
            state: transaction_indicator,
        })
    }

    fn debug(&self) -> String {
        format!("state = {:?}", self.state)
    }

    fn to_vec(&self) -> Vec<u8> {
        Vec::new()
    }
}
