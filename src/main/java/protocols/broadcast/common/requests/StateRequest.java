package protocols.broadcast.common.requests;

import protocols.broadcast.common.utils.VectorClock;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.Arrays;
import java.util.UUID;

public class StateRequest extends ProtoRequest {

    public static final short REQUEST_ID = 204;

    private final UUID msgId;
    private  final VectorClock vc;
    private final byte[] state;

    public StateRequest(UUID msgId, VectorClock vc, byte[] state) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.vc = vc;
        this.state = state;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public VectorClock getVc() {
        return vc;
    }

    public byte[] getState() {
        return state;
    }

    @Override
    public String toString() {
        return "StateRequest{" +
                "msgId=" + msgId +
                ", vc=" + vc +
                '}';
    }
}
