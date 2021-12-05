package protocols.broadcast.synctree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

import java.util.UUID;

public class IHaveTimeout extends ProtoTimer {

    public static final short TIMER_ID = 301;

    private final UUID mid;

    public IHaveTimeout(UUID mid) {
        super(TIMER_ID);
        this.mid = mid;
    }

    public UUID getMid() {
        return mid;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
