package protocols.broadcast.plumtree.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;


public class PendingSync extends ProtoNotification {

    public static final short NOTIFICATION_ID = 901;

    private final Host neighbour;

    public PendingSync(Host neighbour) {
        super(NOTIFICATION_ID);
        this.neighbour = neighbour;
    }

    public Host getNeighbour() {
        return this.neighbour;
    }
}