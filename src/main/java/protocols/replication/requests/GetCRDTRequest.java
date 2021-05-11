package protocols.replication.requests;

import protocols.apps.CRDTApp;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class GetCRDTRequest extends ProtoRequest {

    public static final short REQUEST_ID = 604;

    private final UUID msgId;
    private final Host sender;
    private final String crdtType;
    private final String[] dataType;
    private final String crdtId;


    public GetCRDTRequest(UUID msgId, Host sender, String crdtType, String[] dataType, String crdtId) {
        super(REQUEST_ID);
        this.msgId = msgId;
        this.sender = sender;
        this.crdtType = crdtType;
        this.dataType = dataType;
        this.crdtId = crdtId;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public String getCrdtType() {
        return crdtType;
    }

    public String[] getDataType() {
        return dataType;
    }

    public String getCrdtId() {
        return crdtId;
    }

}
