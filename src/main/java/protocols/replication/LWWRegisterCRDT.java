package protocols.replication;

import crdts.interfaces.RegisterCRDT;
import crdts.operations.Operation;
import crdts.operations.RegisterOperation;
import crdts.utils.VectorClock;
import datatypes.SerializableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.requests.DownstreamRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.time.Instant;
import java.util.UUID;

//TODO: synchronized
public class LWWRegisterCRDT implements RegisterCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(LWWRegisterCRDT.class);

    public enum RegisterOpType{
        ASSIGN
    }

    private static final String CRDT_TYPE = "lww_register";
    private static final String ASSIGN = "assign";

    private final ReplicationKernel kernel;
    private final String crdtId;
    private SerializableType value;
    private Instant ts;

    public LWWRegisterCRDT(ReplicationKernel kernel, String crdtId) {
        this.kernel = kernel;
        this.crdtId = crdtId;
        this.ts = Instant.now();
        this.value = null;
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public SerializableType value() {
        return this.value;
    }

    public void assign(Host sender, SerializableType value) {
        this.ts = Instant.now();
        this.value = value;
        Operation op = new RegisterOperation(sender, 0, null, ASSIGN, crdtId, CRDT_TYPE, value, this.ts);
        UUID id = UUID.randomUUID();
        logger.debug("Downstream assign {} op for {} - {}", value, crdtId, id);
        kernel.downstream(new DownstreamRequest(id, sender, op), (short)0);
    }

    public void upstream(Operation op) {
        RegisterOperation regOp = ((RegisterOperation)op);
        SerializableType value = regOp.getValue();
        Instant timestamp = regOp.getTimestamp();

        if (this.ts.isBefore(timestamp)) {
            this.ts = timestamp;
            this.value = value;
        }
    }

}
