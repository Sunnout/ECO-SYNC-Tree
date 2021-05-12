package protocols.broadcast.plumtree.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;


public class PendingSyncNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 902;

    private final Host neighbour;

    public PendingSyncNotification(Host neighbour) {
        super(NOTIFICATION_ID);
        this.neighbour = neighbour;
    }

    public Host getNeighbour() {
        return this.neighbour;
    }
}
