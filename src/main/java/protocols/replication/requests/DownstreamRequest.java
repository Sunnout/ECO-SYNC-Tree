package protocols.replication.requests;

import crdts.operations.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class DownstreamRequest extends ProtoRequest {

    public static final short REQUEST_ID = 603;

    private final Host sender;
    private final UUID msgId;
    private final Operation op;

    public DownstreamRequest(UUID msgId, Host sender, Operation op) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.op = op;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public Operation getOperation() {
        return op;
    }

}
