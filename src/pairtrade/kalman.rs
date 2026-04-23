/// Scalar Kalman filter for online hedge-ratio (beta) estimation.
///
/// Model
/// -----
///   dy_t = beta_t * dx_t + v_t,   v_t ~ N(0, R)   (observation)
///   beta_t = beta_{t-1} + w_t,    w_t ~ N(0, Q)    (state transition)
///
/// where dx / dy are consecutive log-return differences of the quote /
/// base assets respectively.
#[derive(Debug, Clone)]
pub(super) struct KalmanBeta {
    pub(super) beta: f64,
    pub(super) p: f64, // estimate variance
    q: f64,            // process noise
    r: f64,            // observation noise
    updates: u64,
}

impl KalmanBeta {
    pub(super) fn new(initial_beta: f64, initial_p: f64, q: f64, r: f64) -> Self {
        Self {
            beta: initial_beta,
            p: initial_p,
            q,
            r,
            updates: 0,
        }
    }

    /// Feed one observation pair (dx, dy) where
    ///   dx = log(quote_t) - log(quote_{t-1})
    ///   dy = log(base_t)  - log(base_{t-1})
    /// Returns the updated beta estimate.
    pub(super) fn update(&mut self, dx: f64, dy: f64) -> f64 {
        // Predict
        let p_pred = self.p + self.q;

        // Innovation
        let innovation = dy - self.beta * dx;
        if dx.abs() < 1e-12 {
            // dx ≈ 0 → no information about beta; skip update
            return self.beta;
        }
        let s = dx * dx * p_pred + self.r;

        // Update
        let k = p_pred * dx / s;
        self.beta += k * innovation;
        self.p = (1.0 - k * dx) * p_pred;
        self.beta = self.beta.clamp(0.1, 10.0);
        self.updates += 1;
        self.beta
    }

    pub(super) fn is_warm(&self, min_updates: u64) -> bool {
        self.updates >= min_updates
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converges_to_known_beta() {
        // True beta = 0.6: dy = 0.6 * dx
        let true_beta = 0.6;
        let mut kf = KalmanBeta::new(1.0, 1.0, 1e-5, 1e-3);
        // Feed 500 observations with varying dx (large enough to be informative)
        for i in 1..=500 {
            let dx = 0.01 * ((i as f64) * 0.1).sin();
            let dy = true_beta * dx;
            kf.update(dx, dy);
        }
        assert!(
            (kf.beta - true_beta).abs() < 0.05,
            "expected beta near {}, got {}",
            true_beta,
            kf.beta,
        );
        assert!(kf.is_warm(60));
    }

    #[test]
    fn skip_update_when_dx_zero() {
        let mut kf = KalmanBeta::new(1.0, 1.0, 1e-5, 1e-3);
        let beta_before = kf.beta;
        kf.update(0.0, 0.001);
        assert_eq!(kf.beta, beta_before);
        assert!(!kf.is_warm(1)); // update count not incremented
    }

    #[test]
    fn clamps_to_valid_range() {
        let mut kf = KalmanBeta::new(0.5, 1.0, 1e-5, 1e-3);
        // Push beta toward zero with extreme observations
        for _ in 0..500 {
            kf.update(1.0, -10.0);
        }
        assert!(kf.beta >= 0.1, "beta {} below lower clamp", kf.beta);
    }
}
