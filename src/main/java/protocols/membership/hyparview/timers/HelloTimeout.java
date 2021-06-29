package protocols.membership.hyparview.timers;


import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class HelloTimeout extends ProtoTimer {
    public static final short TIMER_ID = 402;

    public HelloTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
