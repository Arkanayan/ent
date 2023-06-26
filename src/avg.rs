use std::time::Duration;

#[derive(Default, Debug)]
pub struct SlidingAvg<const INVERTED_GAIN: usize> {
    mean: i64,
    avg_deviation: i64,
    num_samples: usize,
}

impl<const INVERTED_GAIN: usize> SlidingAvg<INVERTED_GAIN> {
    pub fn add_sample(&mut self, s: Duration) {
        let mut s = s.as_millis() as i64;

        s *= 64;

        let deviation = if self.num_samples > 0 {
            (self.mean - s).abs()
        } else {
            0
        };

        if self.num_samples < INVERTED_GAIN {
            self.num_samples += 1;
        }

        self.mean += (s - self.mean) / self.num_samples as i64;

        if self.num_samples > 1 {
            // the exact same thing for deviation off the mean except -1 on
            // the samples, because the number of deviation samples always lags
            // behind by 1 (you need to actual samples to have a single deviation
            // sample).
            self.avg_deviation += (deviation - self.avg_deviation) / (self.num_samples - 1) as i64;
        }
    }

    pub fn mean(&self) -> Duration {
        let mean = if self.num_samples > 0 {
            (self.mean + 32) / 64
        } else {
            0
        };

        Duration::from_millis(mean as u64)
    }

    pub fn avg_deviation(&self) -> Duration {
        let deviaton = if self.num_samples > 1 {
            (self.avg_deviation + 32) / 64
        } else {
            0
        };

        Duration::from_millis(deviaton as u64)
    }

	pub fn num_samples(&self) -> usize {
		self.num_samples
	}
}
