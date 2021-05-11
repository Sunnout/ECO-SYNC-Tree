package protocols.apps.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class CreateCRDTsTimer extends ProtoTimer {
    public static final short TIMER_ID = 306;

    public CreateCRDTsTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
