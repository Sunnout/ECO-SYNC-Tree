package protocols.replication.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;
import java.util.UUID;

public class SyncRequest extends ProtoRequest {

    public static final short REQUEST_ID = 608;

    private final UUID msgId;
    private final Host sender;
    private final List<byte[]> ops;

    public SyncRequest(UUID msgId, Host sender, List<byte[]> ops) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.ops = ops;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public List<byte[]> getOperations() {
        return ops;
    }

}
