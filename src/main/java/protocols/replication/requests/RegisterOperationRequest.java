package protocols.replication.requests;

import datatypes.SerializableType;
import protocols.replication.LWWRegisterCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class RegisterOperationRequest extends ProtoRequest {

    public static final short REQUEST_ID = 606;

    private final Host sender;
    private final String crdtId;
    private final LWWRegisterCRDT.RegisterOpType opType;
    private final SerializableType value;

    public RegisterOperationRequest(Host sender, String crdtId, LWWRegisterCRDT.RegisterOpType opType, SerializableType value) {
        super(REQUEST_ID);
        this.sender = sender;
        this.crdtId = crdtId;
        this.opType = opType;
        this.value = value;
    }

    public Host getSender() {
        return sender;
    }

    public String getCrdtId() {
        return crdtId;
    }

    public LWWRegisterCRDT.RegisterOpType getOpType() {
        return opType;
    }

    public SerializableType getValue() {
        return value;
    }
}
