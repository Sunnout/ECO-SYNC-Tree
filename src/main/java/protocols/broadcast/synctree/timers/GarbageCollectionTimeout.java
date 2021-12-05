package protocols.broadcast.synctree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class GarbageCollectionTimeout extends ProtoTimer {

    public static final short TIMER_ID = 303;

    public GarbageCollectionTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
