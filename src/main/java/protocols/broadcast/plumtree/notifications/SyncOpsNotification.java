package protocols.broadcast.plumtree.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;

public class SyncOpsNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 903;

    private final Host neighbour;
    private final List<byte[]> ops;

    public SyncOpsNotification(Host neighbour, List<byte[]> ops) {
        super(NOTIFICATION_ID);
        this.neighbour = neighbour;
        this.ops = ops;
    }

    public Host getNeighbour() {
        return neighbour;
    }

    public List<byte[]> getOperations() {
        return ops;
    }

    @Override
    public String toString() {
        return "SyncOpsNotification{" +
                "neighbour=" + neighbour + ", " +
                "ops=" + ops +
                '}';
    }

}
