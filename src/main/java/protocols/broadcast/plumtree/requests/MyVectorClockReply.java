package protocols.broadcast.plumtree.requests;

import crdts.utils.VectorClock;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class MyVectorClockReply extends ProtoRequest {

    public static final short REQUEST_ID = 901;

    private final UUID msgId;
    private final Host sender;
    private final Host to;
    private final  VectorClock vc;

    public MyVectorClockReply(UUID msgId, Host sender, Host to,  VectorClock vc) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.to = to;
        this.vc = vc;
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
    public VectorClock getVectorClock() {
        return vc;
    }

}
