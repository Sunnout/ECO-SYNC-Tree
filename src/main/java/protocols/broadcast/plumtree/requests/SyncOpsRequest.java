package protocols.broadcast.plumtree.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;
import java.util.UUID;


public class SyncOpsRequest extends ProtoRequest {

    public static final short REQUEST_ID = 904;

    private final UUID msgId;
    private final Host sender;
    private final Host to;
    private final List<byte[]> ops;

    public SyncOpsRequest(UUID msgId, Host sender, Host to, List<byte[]> ops) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.to = to;
        this.ops = ops;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public Host getSender() {
        return sender;
    }

    public Host getTo() {
        return to;
    }

    public List<byte[]> getOperations() {
        return ops;
    }

}
