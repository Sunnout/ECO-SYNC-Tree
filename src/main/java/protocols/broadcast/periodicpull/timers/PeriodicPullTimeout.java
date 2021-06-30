package protocols.broadcast.periodicpull.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PeriodicPullTimeout extends ProtoTimer {

    public static final short TIMER_ID = 491;

    public PeriodicPullTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
