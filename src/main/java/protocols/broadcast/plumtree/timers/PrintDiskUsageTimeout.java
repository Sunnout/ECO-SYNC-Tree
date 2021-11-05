package protocols.broadcast.plumtree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PrintDiskUsageTimeout extends ProtoTimer {

    public static final short TIMER_ID = 307;

    public PrintDiskUsageTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
