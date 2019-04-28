use std::time::Duration;

struct SharpDecrease {
    // Delay in monitoring.
    // We ususally don't want to monitor `now` moment due to delays in transport
    delay: Duration,
    // Monitoring interval in seconds
    interval: Duration,
    // We are comparing interval with previous look_back * interval
    // It will help us to ignore short spikes
    look_back: u8,
}




