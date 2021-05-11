package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class SingleOpTimer extends ProtoTimer {
    public static final short TIMER_ID = 307;

    public SingleOpTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
