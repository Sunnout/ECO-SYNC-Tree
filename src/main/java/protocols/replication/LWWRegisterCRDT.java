package protocols.replication;

import protocols.replication.crdts.interfaces.RegisterCRDT;
import protocols.replication.crdts.operations.Operation;
import protocols.replication.crdts.operations.RegisterOperation;
import protocols.replication.crdts.datatypes.SerializableType;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.replication.crdts.serializers.CRDTSerializer;
import protocols.replication.crdts.serializers.MySerializer;

import java.io.IOException;
import java.time.Instant;

public class LWWRegisterCRDT implements RegisterCRDT, KernelCRDT {

    private static final Logger logger = LogManager.getLogger(LWWRegisterCRDT.class);

    private static final String CRDT_TYPE = "lww_register";
    private static final String ASSIGN = "assign";

    public enum RegisterOpType{
        ASSIGN
    }

    private final String crdtId;
    private SerializableType value;
    private Instant ts;

    public LWWRegisterCRDT(String crdtId) {
        this.crdtId = crdtId;
        this.ts = Instant.now();
        this.value = null;
    }

    public LWWRegisterCRDT(String crdtId, Instant ts, SerializableType value) {
        this.crdtId = crdtId;
        this.ts = ts;
        this.value = value;
    }

    @Override
    public String getCrdtId() {
        return this.crdtId;
    }

    public synchronized SerializableType value() {
        return this.value;
    }

    public synchronized RegisterOperation assignOperation(SerializableType value) {
        return new RegisterOperation(ASSIGN, crdtId, CRDT_TYPE, value, Instant.now());
    }

    public synchronized void upstream(Operation op) {
        RegisterOperation regOp = ((RegisterOperation)op);
        SerializableType value = regOp.getValue();
        Instant timestamp = regOp.getTimestamp();

        if (this.ts.isBefore(timestamp)) {
            this.ts = timestamp;
            this.value = value;
        }
    }

    @Override
    public synchronized void installState(KernelCRDT newCRDT) {
        LWWRegisterCRDT newRegister = (LWWRegisterCRDT) newCRDT;
        this.value = newRegister.value();
        this.ts = newRegister.getInstant();
    }

    public static CRDTSerializer<RegisterCRDT> serializer = new CRDTSerializer<RegisterCRDT>() {
        @Override
        public void serialize(RegisterCRDT registerCRDT, MySerializer[] serializers, ByteBuf out) throws IOException {
            out.writeInt(registerCRDT.getCrdtId().getBytes().length);
            out.writeBytes(registerCRDT.getCrdtId().getBytes());
            out.writeLong(((LWWRegisterCRDT)registerCRDT).getInstant().getEpochSecond());
            out.writeInt(((LWWRegisterCRDT)registerCRDT).getInstant().getNano());
            boolean isNull = ((LWWRegisterCRDT) registerCRDT).value == null;
            out.writeBoolean(isNull);
            if(!isNull)
                serializers[0].serialize(registerCRDT.value(), out);
        }

        @Override
        public RegisterCRDT deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            long epoch = in.readLong();
            int nano = in.readInt();
            boolean isNull = in.readBoolean();
            SerializableType value = null;
            if(!isNull)
                value = (SerializableType) serializers[0].deserialize(in);
            return new LWWRegisterCRDT(new String(crdtId), Instant.ofEpochSecond(epoch, nano), value);
        }
    };

    private Instant getInstant() {
        return this.ts;
    }

}