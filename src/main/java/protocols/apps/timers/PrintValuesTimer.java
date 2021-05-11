package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PrintValuesTimer extends ProtoTimer {
    public static final short TIMER_ID = 309;

    public PrintValuesTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
