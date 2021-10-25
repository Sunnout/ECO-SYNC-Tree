package protocols.replication.requests;

import datatypes.SerializableType;
import protocols.replication.ORMapCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class MapOperationRequest extends ProtoRequest {

    public static final short REQUEST_ID = 608;

    private final String crdtId;
    private final ORMapCRDT.MapOpType opType;
    private final SerializableType key;
    private final SerializableType value;

    public MapOperationRequest(String crdtId, ORMapCRDT.MapOpType opType, SerializableType key, SerializableType value) {
        super(REQUEST_ID);
        this.crdtId = crdtId;
        this.opType = opType;
        this.key = key;
        this.value = value;
    }

    public String getCrdtId() {
        return crdtId;
    }

    public ORMapCRDT.MapOpType getOpType() {
        return opType;
    }

    public SerializableType getKey() {
        return key;
    }

    public SerializableType getValue() {
        return value;
    }
}
