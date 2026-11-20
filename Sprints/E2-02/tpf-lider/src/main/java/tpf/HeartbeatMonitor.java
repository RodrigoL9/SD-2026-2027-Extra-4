package tpf;

public final class HeartbeatMonitor {

    private long lastArrivalMs;
    private long lastSentMs;
    private double ewmaLatencyMs = 50.0;
    private long count = 0;

    public HeartbeatMonitor(long nowMs) {
        this.lastArrivalMs = nowMs;
        this.lastSentMs = nowMs;
    }

    public synchronized void recordHeartbeat(long sentMs, long arrivalMs) {
        this.lastSentMs = sentMs;
        this.lastArrivalMs = arrivalMs;
        this.count++;
        long sample = Math.max(1L, Math.abs(arrivalMs - sentMs));
        if (count == 1) {
            this.ewmaLatencyMs = sample;
        } else {
            this.ewmaLatencyMs = 0.7 * this.ewmaLatencyMs + 0.3 * sample;
        }
    }

    public synchronized long getLastArrivalMs() {
        return lastArrivalMs;
    }

    public synchronized double getEwmaLatencyMs() {
        return ewmaLatencyMs;
    }
}
