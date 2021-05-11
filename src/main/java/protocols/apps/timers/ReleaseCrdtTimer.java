package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ReleaseCrdtTimer extends ProtoTimer {
    public static final short TIMER_ID = 308;

    public ReleaseCrdtTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
