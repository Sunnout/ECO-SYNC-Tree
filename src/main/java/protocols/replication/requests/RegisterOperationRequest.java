package protocols.replication.requests;

import protocols.replication.crdts.datatypes.SerializableType;
import protocols.replication.LWWRegisterCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class RegisterOperationRequest extends ProtoRequest {

    public static final short REQUEST_ID = 606;

    private final String crdtId;
    private final LWWRegisterCRDT.RegisterOpType opType;
    private final SerializableType value;

    public RegisterOperationRequest(String crdtId, LWWRegisterCRDT.RegisterOpType opType, SerializableType value) {
        super(REQUEST_ID);
        this.crdtId = crdtId;
        this.opType = opType;
        this.value = value;
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
