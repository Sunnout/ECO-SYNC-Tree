package protocols.replication.requests;

import protocols.replication.OpCounterCRDT;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class CounterOperationRequest extends ProtoRequest {

    public static final short REQUEST_ID = 605;

    private final Host sender;
    private final String crdtId;
    private final OpCounterCRDT.CounterOpType opType;
    private final int value;

    public CounterOperationRequest(Host sender, String crdtId, OpCounterCRDT.CounterOpType opType, int value) {
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

    public OpCounterCRDT.CounterOpType getOpType() {
        return opType;
    }

    public int getValue() {
        return value;
    }
}
