package protocols.replication.requests;

import datatypes.SerializableType;
import protocols.replication.ORMapCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class MapOperationRequest extends ProtoRequest {

    public static final short REQUEST_ID = 608;

    private final Host sender;
    private final String crdtId;
    private final ORMapCRDT.MapOpType opType;
    private final SerializableType key;
    private final SerializableType value;

    public MapOperationRequest(Host sender, String crdtId, ORMapCRDT.MapOpType opType, SerializableType key, SerializableType value) {
        super(REQUEST_ID);
        this.sender = sender;
        this.crdtId = crdtId;
        this.opType = opType;
        this.key = key;
        this.value = value;
    }

    public Host getSender() {
        return sender;
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
