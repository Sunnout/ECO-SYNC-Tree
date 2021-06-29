package protocols.membership.hyparview.timers;


import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ShuffleTimeout extends ProtoTimer {
    public static final short TIMER_ID = 403;

    public ShuffleTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
