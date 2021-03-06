package protocols.broadcast.synctree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class CheckReceivedTreeMessagesTimeout extends ProtoTimer {

    public static final short TIMER_ID = 305;

    public CheckReceivedTreeMessagesTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
