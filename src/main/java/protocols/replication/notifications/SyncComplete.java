package protocols.replication.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class SyncComplete extends ProtoNotification {

    public static final short NOTIFICATION_ID = 603;

    private final Host neighbour;

    public SyncComplete(Host neighbour) {
        super(NOTIFICATION_ID);
        this.neighbour = neighbour;
    }

    public Host getNeighbour() {
        return this.neighbour;
    }
}

