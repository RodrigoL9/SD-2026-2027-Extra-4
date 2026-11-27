package tpf;

public final class HeartbeatMonitor {

    private long ultimoHbMs;
    private ReplicaRegistry.State state;

    public HeartbeatMonitor(long nowMs) {
        this.ultimoHbMs = nowMs;
        this.state = ReplicaRegistry.State.VIVA;
    }

    public synchronized ReplicaRegistry.State onHeartbeat(long nowMs) {
        this.ultimoHbMs = nowMs;
        ReplicaRegistry.State prev = this.state;
        this.state = ReplicaRegistry.State.VIVA;
        return prev != ReplicaRegistry.State.VIVA ? prev : null;
    }

    public synchronized ReplicaRegistry.State atualizarEstado(long agoraMs) {
        long elapsed = agoraMs - ultimoHbMs;
        ReplicaRegistry.State novo;
        if (elapsed >= Wire.MORTA_MS) {
            novo = ReplicaRegistry.State.MORTA;
        } else if (elapsed >= Wire.SUSPEITA_MS) {
            novo = ReplicaRegistry.State.SUSPEITA;
        } else {
            novo = ReplicaRegistry.State.VIVA;
        }
        if (novo != this.state) {
            ReplicaRegistry.State antigo = this.state;
            this.state = novo;
            return antigo;
        }
        return null;
    }

    public synchronized ReplicaRegistry.State getState() {
        return state;
    }

    public synchronized long getUltimoHbMs() {
        return ultimoHbMs;
    }
}