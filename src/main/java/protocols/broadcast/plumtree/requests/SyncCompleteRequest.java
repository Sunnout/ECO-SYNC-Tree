package protocols.broadcast.plumtree.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class SyncCompleteRequest extends ProtoRequest {

    public static final short REQUEST_ID = 903;

    private final UUID msgId;
    private final Host sender;
    private final Host neighbour;

    public SyncCompleteRequest(UUID msgId, Host sender, Host neighbour) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.neighbour = neighbour;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public Host getSender() {
        return sender;
    }

    public Host getNeighbour() {
        return neighbour;
    }

}