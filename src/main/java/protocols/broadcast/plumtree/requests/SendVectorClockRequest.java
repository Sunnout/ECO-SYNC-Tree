package protocols.broadcast.plumtree.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class SendVectorClockRequest extends ProtoRequest {

    public static final short REQUEST_ID = 901;

    private final UUID msgId;
    private final Host sender;
    private final Host to;

    public SendVectorClockRequest(UUID msgId, Host sender, Host to) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.to = to;
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


}
