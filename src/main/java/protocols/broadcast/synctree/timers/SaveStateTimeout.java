package protocols.broadcast.synctree.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class SaveStateTimeout extends ProtoTimer {

    public static final short TIMER_ID = 306;

    public SaveStateTimeout() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
