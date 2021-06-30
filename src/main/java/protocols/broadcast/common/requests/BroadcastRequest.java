package protocols.broadcast.common.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class BroadcastRequest extends ProtoRequest {

    public static final short REQUEST_ID = 202;

    private final Host sender;
    private final UUID msgId;
    private final byte[] msg;

    public BroadcastRequest(UUID msgId, Host sender, byte[] msg) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.msg = msg;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public byte[] getMsg() {
        return msg;
    }

}
