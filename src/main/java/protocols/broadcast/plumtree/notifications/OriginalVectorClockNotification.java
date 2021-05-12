package protocols.broadcast.plumtree.notifications;

import crdts.utils.VectorClock;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;


public class OriginalVectorClockNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 901;

    private final Host neighbour;
    private final VectorClock vc;

    public OriginalVectorClockNotification(Host neighbour, VectorClock vc) {
        super(NOTIFICATION_ID);
        this.neighbour = neighbour;
        this.vc = vc;
    }

    public Host getNeighbour() {
        return this.neighbour;
    }

    public VectorClock getVectorClock() {
        return vc;
    }

}
