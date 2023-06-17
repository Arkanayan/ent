
#[derive(Debug, Default)]
pub struct ThruputCounters {
    /// Counts protocol chatter
    pub protocol: ChannelCounter,
    /// Counts exchanged block data, excludes the header, which counts as protocol chatter.
    pub payload: ChannelCounter
}

impl ThruputCounters {
    pub fn reset(&mut self) {
        self.protocol.reset();
        self.payload.reset();
    }
}

#[derive(Debug, Default)]
pub struct ChannelCounter {
    pub up: Counter,
    pub down: Counter
}

impl ChannelCounter {
    pub fn reset(&mut self) {
        self.up.reset();
        self.down.reset();
    }
}

#[derive(Default, Debug)]
pub struct Counter {
    // Total counter
    total: u64,
    // The accumulator for this round
    round: u64,
    // sliding average of 5 sec
    avg: f64,
    // peak
    peak: f64
}

impl Counter {
    pub fn add(&mut self, bytes: u64) {
        self.round += bytes;
        self.total += bytes
    }

    /// Finishes counting this round and updates 5 second moving average
    /// # Important
    /// 
    /// Expected to be called once a second
    pub fn reset(&mut self) {
        self.avg = (self.round * 4 / 5) as f64 + (self.round / 5) as f64;
        self.round = 0;

        if self.avg > self.peak {
            self.peak = self.avg;
        }
    }

    pub fn round(&self) -> u64 {
        self.round
    }

    pub fn avg(&self) -> u64 {
        self.avg.round() as u64
    }

    pub fn total(&self) -> u64 {
        self.total
    }
}
