use std::collections::BTreeMap;

use crate::error::{err, Result};

#[derive(Debug, Clone)]
pub struct OpenSlice {
    pub tokens: u64,
    pub used: u64,
    pub memory_mb: u64,
    pub memory_lent: u64,
    pub pids: u64,
    pub pids_lent: u64,
    pub seconds: u64,
}

#[derive(Debug, Clone)]
pub struct Purse {
    pub cap: u64,
    pub memory_cap: u64,
    pub pids_cap: u64,
    pub period_ms: u64,
    pub period_start: Option<u64>,
    pub available: u64,
    pub memory_held: u64,
    pub pids_held: u64,
    pub resets: u64,
    pub open: BTreeMap<String, OpenSlice>,
}

impl Purse {
    pub fn new(cap: u64, memory_cap: u64, pids_cap: u64, period_ms: u64) -> Self {
        Self {
            cap,
            memory_cap,
            pids_cap,
            period_ms,
            period_start: None,
            available: cap,
            memory_held: 0,
            pids_held: 0,
            resets: 0,
            open: BTreeMap::new(),
        }
    }

    pub fn held(&self) -> u64 {
        self.open.values().map(|s| s.tokens).sum()
    }

    /// Tokens that are not free: live reservations plus finished spend.
    pub fn spent(&self) -> u64 {
        self.cap.saturating_sub(self.available)
    }

    pub fn due(&self, now: u64) -> bool {
        match self.period_start {
            None => true,
            Some(start) => now.saturating_sub(start) >= self.period_ms,
        }
    }

    /// New grant is `cap` minus tokens still held by live admits.
    /// The reset does not add those tokens back, so an exit cannot refund them twice.
    pub fn apply_reset(&mut self, now: u64) {
        let held = self.held();
        self.available = self.cap.saturating_sub(held);
        self.period_start = Some(now);
        self.resets += 1;
    }

    pub fn debit_root(
        &mut self,
        id: &str,
        tokens: u64,
        seconds: u64,
        memory_mb: u64,
        pids: u64,
    ) -> Result<()> {
        if self.open.contains_key(id) {
            return Ok(());
        }
        if self.available < tokens {
            return Err(err("purse"));
        }
        if self.memory_held.saturating_add(memory_mb) > self.memory_cap {
            return Err(err("memory"));
        }
        if self.pids_held.saturating_add(pids) > self.pids_cap {
            return Err(err("pids"));
        }
        self.available -= tokens;
        self.memory_held += memory_mb;
        self.pids_held += pids;
        self.open.insert(
            id.to_string(),
            OpenSlice {
                tokens,
                used: 0,
                memory_mb,
                memory_lent: 0,
                pids,
                pids_lent: 0,
                seconds,
            },
        );
        Ok(())
    }

    pub fn debit_parent(
        &mut self,
        parent: &str,
        child: &str,
        tokens: u64,
        seconds: u64,
        memory_mb: u64,
        pids: u64,
    ) -> Result<()> {
        if self.open.contains_key(child) {
            return Ok(());
        }
        let parent_slice = self
            .open
            .get(parent)
            .ok_or_else(|| err("parent is not holding a slice"))?;
        let token_left = parent_slice.tokens.saturating_sub(parent_slice.used);
        let mem_left = parent_slice
            .memory_mb
            .saturating_sub(parent_slice.memory_lent);
        let pid_left = parent_slice.pids.saturating_sub(parent_slice.pids_lent);
        if token_left < tokens || mem_left < memory_mb || pid_left < pids {
            return Err(err("purse"));
        }
        let parent_slice = self.open.get_mut(parent).expect("just checked");
        parent_slice.used += tokens;
        parent_slice.memory_lent += memory_mb;
        parent_slice.pids_lent += pids;
        self.open.insert(
            child.to_string(),
            OpenSlice {
                tokens,
                used: 0,
                memory_mb,
                memory_lent: 0,
                pids,
                pids_lent: 0,
                seconds,
            },
        );
        Ok(())
    }

    /// `release_life` returns memory and pids. Token refund is whatever the exit recorded.
    pub fn credit_exit(&mut self, id: &str, refund_tokens: u64, parent: Option<&str>) {
        let Some(slice) = self.open.remove(id) else {
            return;
        };
        let refund = refund_tokens.min(slice.tokens);
        if let Some(pid) = parent {
            if let Some(p) = self.open.get_mut(pid) {
                p.used = p.used.saturating_sub(refund);
                p.memory_lent = p.memory_lent.saturating_sub(slice.memory_mb);
                p.pids_lent = p.pids_lent.saturating_sub(slice.pids);
            }
        } else {
            self.available = self.available.saturating_add(refund);
            self.memory_held = self.memory_held.saturating_sub(slice.memory_mb);
            self.pids_held = self.pids_held.saturating_sub(slice.pids);
        }
    }

    pub fn note_used(&mut self, id: &str, tokens: u64) {
        if let Some(slice) = self.open.get_mut(id) {
            slice.used = slice.used.saturating_add(tokens).min(slice.tokens);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reset_does_not_refund_a_live_slice_twice() {
        let mut p = Purse::new(1000, 100, 10, 86_400_000);
        p.apply_reset(0);
        p.debit_root("a", 400, 10, 1, 1).unwrap();
        p.apply_reset(86_400_000);
        assert_eq!(p.available, 600, "live 400 stays held");
        assert_eq!(p.held(), 400);
        // Exit refunds the unused remainder once. Used 100, refund 300.
        p.credit_exit("a", 300, None);
        assert_eq!(p.available, 900);
        assert_eq!(p.held(), 0);
        // A second credit is a no-op.
        p.credit_exit("a", 300, None);
        assert_eq!(p.available, 900);
    }

    #[test]
    fn finished_spend_does_not_survive_reset() {
        let mut p = Purse::new(1000, 100, 10, 1000);
        p.apply_reset(0);
        p.debit_root("a", 400, 10, 1, 1).unwrap();
        p.credit_exit("a", 0, None);
        assert_eq!(p.available, 600);
        p.apply_reset(1000);
        assert_eq!(p.available, 1000);
    }

    #[test]
    fn crash_keeps_the_token_slice_and_returns_memory() {
        let mut p = Purse::new(1000, 100, 10, 1000);
        p.apply_reset(0);
        p.debit_root("a", 400, 10, 8, 2).unwrap();
        p.credit_exit("a", 0, None);
        assert_eq!(p.available, 600);
        assert_eq!(p.memory_held, 0);
        assert_eq!(p.pids_held, 0);
        assert_eq!(p.spent(), 400);
    }
}
