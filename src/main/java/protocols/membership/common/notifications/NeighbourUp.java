package protocols.membership.common.notifications;

import java.util.HashSet;
import java.util.Set;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class NeighbourUp extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    private final Set<Host> neighbours;

    public NeighbourUp(Host neighbour) {
        super(NOTIFICATION_ID);
        this.neighbours = new HashSet<>();
        neighbours.add(neighbour);
    }
    
    public void addNeighbour(Host neighbour) {
    	this.neighbours.add(neighbour);
    }

    public Set<Host> getNeighbours() {
        return new HashSet<>(this.neighbours);
    }

    public Host getNeighbour() {
        return this.neighbours.iterator().next();
    }

    public int getLength() {
    	return this.neighbours.size();
    }
}
