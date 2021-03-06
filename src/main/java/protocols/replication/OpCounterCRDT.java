package protocols.replication;

import protocols.replication.crdts.interfaces.CounterCRDT;
import protocols.replication.crdts.operations.CounterOperation;
import protocols.replication.crdts.operations.Operation;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.crdts.serializers.CRDTSerializer;
import protocols.replication.crdts.serializers.MySerializer;

import java.math.BigInteger;

public class OpCounterCRDT implements CounterCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(OpCounterCRDT.class);

    private static final String CRDT_TYPE = "counter";
    private static final String INCREMENT = "inc";
    private static final String DECREMENT = "dec";

    public enum CounterOpType{
        INCREMENT,
        DECREMENT,
        INCREMENT_BY,
        DECREMENT_BY
    }

    private final String crdtId;
    private BigInteger c;

    public OpCounterCRDT(String crdtId) {
        this.crdtId = crdtId;
        this.c = BigInteger.ZERO;
    }

    public OpCounterCRDT(String crdtId, int value) {
        this.crdtId = crdtId;
        this.c = BigInteger.valueOf(value);
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public synchronized int value() {
        return this.c.intValue();
    }

    public synchronized CounterOperation incrementOperation() {
        return this.incrementByOperation(1);
    }

    public synchronized CounterOperation incrementByOperation(int v) {
        return new CounterOperation(INCREMENT, crdtId, CRDT_TYPE, v);
    }

    public synchronized CounterOperation decrementOperation() {
        return this.decrementByOperation(1);
    }

    public synchronized CounterOperation decrementByOperation(int v) {
        return new CounterOperation(DECREMENT, crdtId, CRDT_TYPE, v);
    }

    public synchronized void upstream(Operation op) {
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

    public static CRDTSerializer<CounterCRDT> serializer = new CRDTSerializer<CounterCRDT>() {
        @Override
        public void serialize(CounterCRDT counterCRDT, MySerializer[] serializers, ByteBuf out) {
            out.writeInt(counterCRDT.getCrdtId().getBytes().length);
            out.writeBytes(counterCRDT.getCrdtId().getBytes());
            out.writeInt(counterCRDT.value());
        }

        @Override
        public CounterCRDT deserialize(MySerializer[] serializers, ByteBuf in) {
            int size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            int value = in.readInt();
            return new OpCounterCRDT(new String(crdtId), value);
        }
    };
}
