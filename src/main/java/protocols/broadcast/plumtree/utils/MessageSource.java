package protocols.broadcast.plumtree.utils;

import pt.unl.fct.di.novasys.network.data.Host;

public class MessageSource {

    public Host peer;

    public MessageSource(Host peer) {
        this.peer = peer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MessageSource that = (MessageSource) o;
        return peer.equals(that.peer);
    }

    @Override
    public int hashCode() {
        return peer.hashCode();
    }
}
