package protocols.broadcast.plumtree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class GracePeriodTimeout extends ProtoTimer {

    public static final short TIMER_ID = 303;

    public GracePeriodTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
