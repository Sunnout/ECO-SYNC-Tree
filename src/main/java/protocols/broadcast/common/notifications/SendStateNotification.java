package protocols.broadcast.common.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.util.UUID;

public class SendStateNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 202;

    private final UUID msgId;

    public SendStateNotification(UUID msgId) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
    }


    public UUID getMsgId() {
        return msgId;
    }

    @Override
    public String toString() {
        return "SendStateNotification{" +
                "msgId=" + msgId +
                '}';
    }
}
