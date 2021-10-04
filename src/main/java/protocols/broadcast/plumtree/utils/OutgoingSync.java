package protocols.broadcast.plumtree.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class OutgoingSync implements TreeSync {

    private Host host;
    private UUID msgId;

    public OutgoingSync(Host host, UUID msgId) {
        this.host = host;
        this.msgId = msgId;
    }

    public OutgoingSync(Host host) {
        this.host = host;
        this.msgId = null;
    }

    @Override
    public Host getHost() {
        return this.host;
    }


    public UUID getMsgId() {
        return msgId;
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
                ", msgId=" + msgId +
                '}';
    }
}
