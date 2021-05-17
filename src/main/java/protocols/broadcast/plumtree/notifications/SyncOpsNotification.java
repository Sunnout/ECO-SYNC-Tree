package protocols.broadcast.plumtree.notifications;

import crdts.utils.VectorClock;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;

public class SyncOpsNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 904;

    private final Host neighbour;
    private final VectorClock vc;
    private final List<byte[]> ops;

    public SyncOpsNotification(Host neighbour, VectorClock vc, List<byte[]> ops) {
        super(NOTIFICATION_ID);
        this.neighbour = neighbour;
        this.vc = vc;
        this.ops = ops;
    }

    public Host getNeighbour() {
        return neighbour;
    }

    public VectorClock getVectorClock() {
        return vc;
    }

    public List<byte[]> getOperations() {
        return ops;
    }

}
