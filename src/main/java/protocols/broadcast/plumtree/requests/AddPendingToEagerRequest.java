package protocols.broadcast.plumtree.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class AddPendingToEagerRequest extends ProtoRequest {

    public static final short REQUEST_ID = 903;

    private final UUID msgId;
    private final Host sender;

    public AddPendingToEagerRequest(UUID msgId, Host sender) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public Host getSender() {
        return sender;
    }

    @Override
    public String toString() {
        return "AddPendingToEagerRequest{" +
                "sender=" + sender +
                '}';
    }

}
