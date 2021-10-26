package protocols.broadcast.common.notifications;

import protocols.broadcast.common.utils.VectorClock;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.util.UUID;

public class SendStateNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 203;

    private final UUID msgId;
    private final VectorClock vc;

    public SendStateNotification(UUID msgId, VectorClock vc) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.vc = vc;
    }


    public UUID getMsgId() {
        return msgId;
    }

    public VectorClock getVc() {
        return vc;
    }

    @Override
    public String toString() {
        return "SendStateNotification{" +
                "msgId=" + msgId +
                ", vc=" + vc +
                '}';
    }
}
