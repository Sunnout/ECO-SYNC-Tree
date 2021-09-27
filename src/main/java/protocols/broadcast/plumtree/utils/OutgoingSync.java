package protocols.broadcast.plumtree.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class OutgoingSync implements TreeSync {

    private Host host;
    private boolean neighUp;
    private UUID msgId;
    private String cause;

    public OutgoingSync(Host host, boolean neighUp, UUID msgId, String cause) {
        this.host = host;
        this.neighUp = neighUp;
        this.msgId = msgId;
        this.cause = cause;
    }

    public OutgoingSync(Host host) {
        this.host = host;
        this.neighUp = false;
        this.msgId = null;
        this.cause = null;
    }

    @Override
    public Host getHost() {
        return this.host;
    }

    public boolean isNeighUp() {
        return neighUp;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public String getCause() {
        return cause;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof OutgoingSync)) {
            return false;
        } else {
            OutgoingSync o = (OutgoingSync)other;
            return o.getHost().equals(this.host);
        }
    }

    @Override
    public int hashCode() {
        return this.host.hashCode();
    }

    @Override
    public String toString() {
        return "OutgoingSync{" +
                "host=" + host +
                ", neighUp=" + neighUp +
                ", msgId=" + msgId +
                ", cause='" + cause + '\'' +
                '}';
    }
}
