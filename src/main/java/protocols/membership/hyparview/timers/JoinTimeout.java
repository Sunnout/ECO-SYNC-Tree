package protocols.membership.hyparview.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinTimeout extends ProtoTimer {
    public static final short TIMER_ID = 401;

    private Host contact;
    private int count;

    public JoinTimeout(Host contact) {
        super(TIMER_ID);
        this.contact = contact;
        this.count = 1;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

    public Host getContact() {
        return this.contact;
    }

    public void incCount() {
        count++;
    }

    public int getCount() {
        return count;
    }
}
