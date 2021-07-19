package protocols.broadcast.common.notifications;

import crdts.utils.VectorClock;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class VectorClockNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 901;

    private UUID mid;
    private final Host neighbour;
    private final VectorClock vc;

    public VectorClockNotification(UUID mid, Host neighbour, VectorClock vc) {
        super(NOTIFICATION_ID);
        this.mid = mid;
        this.neighbour = neighbour;
        this.vc = vc;
    }

    public UUID getMid() {
        return this.mid;
    }

    public Host getNeighbour() {
        return this.neighbour;
    }

    public VectorClock getVectorClock() {
        return vc;
    }

    @Override
    public String toString() {
        return "VectorClockNotification{" +
                "mid=" + mid +
                ", neighbour=" + neighbour +
                ", vc=" + vc +
                '}';
    }
}
