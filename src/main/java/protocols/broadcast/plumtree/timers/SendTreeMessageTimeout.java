package protocols.broadcast.plumtree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class SendTreeMessageTimeout extends ProtoTimer {

    public static final short TIMER_ID = 304;

    public SendTreeMessageTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
