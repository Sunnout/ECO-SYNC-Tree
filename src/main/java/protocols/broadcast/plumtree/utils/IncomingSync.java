package protocols.broadcast.plumtree.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class IncomingSync implements TreeSync{

    private Host host;
    private UUID mid;


    public IncomingSync(Host host, UUID mid) {
        this.host = host;
        this.mid = mid;
    }

    @Override
    public Host getHost() {
        return this.host;
    }

    public UUID getMid() {
        return mid;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof IncomingSync)) {
            return false;
        } else {
            IncomingSync o = (IncomingSync)other;
            return o.getMid().equals(this.mid) && o.getHost().equals(this.host);
        }
    }

    @Override
    public int hashCode() {
        return this.host.hashCode() + this.mid.hashCode();
    }

    @Override
    public String toString() {
        return "IncomingSync{" +
                "host=" + host +
                ", mid=" + mid +
                '}';
    }
}
