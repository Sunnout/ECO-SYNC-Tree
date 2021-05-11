package protocols.replication.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class ReleaseCRDTRequest extends ProtoRequest {

    public static final short REQUEST_ID = 605;

    private final UUID msgId;
    private final Host sender;
    private final String crdtId;


    public ReleaseCRDTRequest(UUID msgId, Host sender, String crdtId) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.crdtId = crdtId;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public String getCrdtId() {
        return crdtId;
    }

}
