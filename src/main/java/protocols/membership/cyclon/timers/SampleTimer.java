package protocols.membership.cyclon.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class SampleTimer extends ProtoTimer {

    public static final short TIMER_ID = 1004;

    public SampleTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
