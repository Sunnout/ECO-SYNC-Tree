package protocols.replication.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class CRDTAlreadyExistsNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 601;

    private final UUID msgId;
    private final String crdtId;

    public CRDTAlreadyExistsNotification(UUID msgId, String crdtId) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.crdtId = crdtId;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public String getCrdtId() {
        return crdtId;
    }
}
