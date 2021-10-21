package protocols.replication;

import crdts.interfaces.CounterCRDT;
import crdts.operations.CounterOperation;
import crdts.operations.Operation;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.requests.DownstreamRequest;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyCRDTSerializer;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class OpCounterCRDT implements CounterCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(OpCounterCRDT.class);

    public enum CounterOpType{
        INCREMENT,
        DECREMENT,
        INCREMENT_BY,
        DECREMENT_BY
    }

    private static final String CRDT_TYPE = "counter";
    private static final String INCREMENT = "inc";
    private static final String DECREMENT = "dec";

    private final CRDTCommunicationInterface kernel;
    private final String crdtId;
    private BigInteger c;

    public OpCounterCRDT(CRDTCommunicationInterface kernel, String crdtId) {
        this.kernel = kernel;
        this.crdtId = crdtId;
        this.c = BigInteger.ZERO;
    }

    public OpCounterCRDT(CRDTCommunicationInterface kernel, String crdtId, int value) {
        this.kernel = kernel;
        this.crdtId = crdtId;
        this.c = BigInteger.valueOf(value);
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public int value() {
        return this.c.intValue();
    }

    public void increment(Host sender) {
        this.incrementBy(sender, 1);
    }

    public void incrementBy(Host sender, int v) {
        this.c = this.c.add(BigInteger.valueOf(v));
        Operation op = new CounterOperation(INCREMENT, crdtId, CRDT_TYPE, v);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream incrementBy {} op for {} - {}", v, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public void decrement(Host sender) {
        this.decrementBy(sender, 1);
    }

    public void decrementBy(Host sender, int v) {
        this.c = this.c.subtract(BigInteger.valueOf(v));
        Operation op = new CounterOperation(DECREMENT, crdtId, CRDT_TYPE, v);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream decrementBy {} op for {} - {}", v, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public void upstream(Operation op) {
        String opType = op.getOpType();
        int value = ((CounterOperation)op).getValue();

        if(opType.equals(INCREMENT))
            this.c = this.c.add(BigInteger.valueOf(value));
        else if(opType.equals(DECREMENT))
            this.c = this.c.subtract(BigInteger.valueOf(value));
    }

    @Override
    public synchronized void installState(KernelCRDT newCRDT) {
        this.c = BigInteger.valueOf(((OpCounterCRDT) newCRDT).value());
    }

    public static MyCRDTSerializer<CounterCRDT> serializer = new MyCRDTSerializer<CounterCRDT>() {
        @Override
        public void serialize(CounterCRDT counterCRDT, MySerializer[] serializers, ByteBuf out) {
            out.writeInt(counterCRDT.getCrdtId().getBytes().length);
            out.writeBytes(counterCRDT.getCrdtId().getBytes());
            out.writeInt(counterCRDT.value());
        }

        @Override
        public CounterCRDT deserialize(CRDTCommunicationInterface kernel, MySerializer[] serializers, ByteBuf in) {
            int size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            int value = in.readInt();
            return new OpCounterCRDT(kernel, new String(crdtId), value);
        }
    };
}
