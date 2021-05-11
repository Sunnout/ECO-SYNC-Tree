package protocols.replication;

import crdts.interfaces.CounterCRDT;
import crdts.operations.CounterOperation;
import crdts.operations.Operation;
import crdts.utils.VectorClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.requests.DownstreamRequest;
import pt.unl.fct.di.novasys.network.data.Host;

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

    private final ReplicationKernel kernel;
    private final String crdtId;
    private BigInteger c;

    public OpCounterCRDT(ReplicationKernel kernel, String crdtId) {
        this.kernel = kernel;
        this.crdtId = crdtId;
        this.c = BigInteger.ZERO;
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public int value() {
        return this.c.intValue();
    }

    public synchronized void increment(Host sender) {
        this.incrementBy(sender, 1);
    }

    public synchronized void incrementBy(Host sender, int v) {
        this.c = this.c.add(BigInteger.valueOf(v));
        Operation op = new CounterOperation(sender, 0, INCREMENT, crdtId, CRDT_TYPE, v);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream incrementBy {} op for {} - {}", v, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public synchronized void decrement(Host sender) {
        this.decrementBy(sender, 1);
    }

    public synchronized void decrementBy(Host sender, int v) {
        this.c = this.c.subtract(BigInteger.valueOf(v));
        Operation op = new CounterOperation(sender, 0, DECREMENT, crdtId, CRDT_TYPE, v);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream decrementBy {} op for {} - {}", v, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public synchronized void upstream(Operation op) {
        String opType = op.getOpType();
        int value = ((CounterOperation)op).getValue();

        if(opType.equals(INCREMENT))
            this.c = this.c.add(BigInteger.valueOf(value));
        else if(opType.equals(DECREMENT))
            this.c = this.c.subtract(BigInteger.valueOf(value));
    }
}
