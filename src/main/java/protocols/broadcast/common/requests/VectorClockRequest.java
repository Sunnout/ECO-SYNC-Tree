package protocols.broadcast.common.requests;

import crdts.utils.VectorClock;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;


public class VectorClockRequest extends ProtoRequest {

    public static final short REQUEST_ID = 902;

    private final UUID msgId;
    private final Host sender;
    private final Host to;
    private final VectorClock vc;

    public VectorClockRequest(UUID msgId, Host sender, Host to, VectorClock vc) {
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

    @Override
    public String toString() {
        return "VectorClockRequest{" +
                "mid=" + msgId +
                ", sender=" + sender +
                ", to=" + to +
                ", vc=" + vc +
                '}';
    }

}
