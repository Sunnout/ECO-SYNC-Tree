package protocols.broadcast.common.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

import java.util.UUID;

public class StateRequest extends ProtoRequest {

    public static final short REQUEST_ID = 204;

    private final UUID msgId;
    private final byte[] state;

    public StateRequest(UUID msgId, byte[] state) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.state = state;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public byte[] getState() {
        return state;
    }

}
