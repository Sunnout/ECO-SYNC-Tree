package protocols.broadcast.common.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class DeliverNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 201;

    private final Host sender;
    private final UUID msgId;
    private final byte[] msg;
    private final boolean fromSync;

    public DeliverNotification(UUID msgId, Host sender, byte[] msg, boolean fromSync) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.msg = msg;
        this.fromSync = fromSync;
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

    public boolean isFromSync() {
        return fromSync;
    }


    @Override
    public String toString() {
        return "DeliverNotification{" +
                "msgId=" + msgId +
                ", sender=" + sender +
                ", fromSync=" + fromSync +
                '}';
    }
}
