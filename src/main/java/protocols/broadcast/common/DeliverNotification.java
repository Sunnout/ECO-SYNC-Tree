package protocols.broadcast.common;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class DeliverNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 201;

    private final Host sender;
    private final UUID msgId;
    private final byte[] msg;

    public DeliverNotification(UUID msgId, Host sender, byte[] msg) {
        super(NOTIFICATION_ID);
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
