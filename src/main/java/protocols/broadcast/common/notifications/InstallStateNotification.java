package protocols.broadcast.common.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.util.UUID;

public class InstallStateNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 202;

    private final UUID msgId;
    private final byte[] state;

    public InstallStateNotification(UUID msgId, byte[] state) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.state = state;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public byte[] getState() {
        return state;
    }

    @Override
    public String toString() {
        return "SendStateNotification{" +
                "msgId=" + msgId +
                '}';
    }
}
