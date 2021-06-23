package protocols.broadcast.plumtree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReconnectTimeout extends ProtoTimer {

    public static final short TIMER_ID = 302;

    private final Host host;

    public ReconnectTimeout(Host host) {
        super(TIMER_ID);
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
