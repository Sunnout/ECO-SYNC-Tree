package protocols.membership.hyparview.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;

public class JoinTimeout extends ProtoTimer {
    public static final short TIMER_CODE = 401;

    private List<Host> contacts;
    private int count;

    public JoinTimeout(List<Host> contacts) {
        super(JoinTimeout.TIMER_CODE);
        this.contacts = contacts;
        this.count = 1;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

    public List<Host> getContacts() {
        return contacts;
    }

    public void incCount() {
        count ++;
    }

    public int getCount() {
        return count;
    }
}
