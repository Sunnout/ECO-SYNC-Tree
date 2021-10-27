package protocols.replication.notifications;

import protocols.replication.crdts.interfaces.GenericCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

import java.util.UUID;

public class ReturnCRDTNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 602;

    private final UUID msgId;
    private final GenericCRDT crdt;

    public ReturnCRDTNotification(UUID msgId, GenericCRDT crdt) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.crdt = crdt;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public GenericCRDT getCrdt() {
        return crdt;
    }
}
