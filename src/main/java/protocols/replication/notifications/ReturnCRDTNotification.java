package protocols.replication.notifications;

import crdts.interfaces.GenericCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class ReturnCRDTNotification extends ProtoNotification {
    public static final short NOTIFICATION_ID = 602;

    private final Host sender;
    private final UUID msgId;
    private final GenericCRDT crdt;

    public ReturnCRDTNotification(UUID msgId, Host sender, GenericCRDT crdt) {
        super(NOTIFICATION_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.crdt = crdt;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public GenericCRDT getCrdt() {
        return crdt;
    }
}
