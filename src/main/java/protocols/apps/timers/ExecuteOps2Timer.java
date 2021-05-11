package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ExecuteOps2Timer extends ProtoTimer {
    public static final short TIMER_ID = 305;

    public ExecuteOps2Timer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
