package protocols.broadcast.common.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class SendVectorClockNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 904;

    private UUID mid;
    private final Host neighbour;

    public SendVectorClockNotification(UUID mid, Host neighbour) {
        super(NOTIFICATION_ID);
        this.mid = mid;
        this.neighbour = neighbour;
    }

    public UUID getMid() {
        return this.mid;
    }

    public Host getNeighbour() {
        return this.neighbour;
    }

    @Override
    public String toString() {
        return "SendVectorClockNotification{" +
                "mid=" + mid +
                ", neighbour=" + neighbour +
                '}';
    }
}
