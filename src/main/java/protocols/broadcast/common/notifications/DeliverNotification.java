package protocols.broadcast.common.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class DeliverNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 201;

    private final UUID msgId;
    private final byte[] msg;

    public DeliverNotification(UUID msgId, byte[] msg) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.msg = msg;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public byte[] getMsg() {
        return msg;
    }

    @Override
    public String toString() {
        return "DeliverNotification{" +
                "msgId=" + msgId +
                '}';
    }
}
