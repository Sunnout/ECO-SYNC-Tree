package protocols.replication.requests;

import datatypes.SerializableType;
import protocols.replication.ORSetCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class SetOperationRequest extends ProtoRequest {

    public static final short REQUEST_ID = 607;

    private final String crdtId;
    private final ORSetCRDT.SetOpType opType;
    private final SerializableType value;

    public SetOperationRequest(String crdtId, ORSetCRDT.SetOpType opType, SerializableType value) {
        super(REQUEST_ID);
        this.crdtId = crdtId;
        this.opType = opType;
        this.value = value;
    }

    public String getCrdtId() {
        return crdtId;
    }

    public ORSetCRDT.SetOpType getOpType() {
        return opType;
    }

    public SerializableType getValue() {
        return value;
    }
}
