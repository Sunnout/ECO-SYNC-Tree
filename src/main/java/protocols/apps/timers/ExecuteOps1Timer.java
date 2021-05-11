package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ExecuteOps1Timer extends ProtoTimer {
    public static final short TIMER_ID = 301;

    public ExecuteOps1Timer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
