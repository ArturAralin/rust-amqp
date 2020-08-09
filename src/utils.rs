use bytes::Bytes;

pub const SYMBOL_SPACE: u8 = 0x20;
pub const SYMBOL_NEW_LINE: u8 = 0x0A;

pub fn find_idx(bytes: &Bytes, target: u8) -> Option<usize> {
  for (idx, b) in bytes.into_iter().enumerate() {
    if *b == target {
      return Some(idx);
    }
  }

  None
}

