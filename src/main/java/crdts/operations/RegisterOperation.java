package crdts.operations;

import datatypes.SerializableType;
import io.netty.buffer.ByteBuf;

import serializers.CRDTOpSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.time.Instant;


public class RegisterOperation extends Operation {

    private final SerializableType value;
    private final Instant timestamp;

    public RegisterOperation(String opType, String crdtId, String crdtType, SerializableType value, Instant timestamp) {
        super(opType, crdtId, crdtType);
        this.value = value;
        this.timestamp = timestamp;
    }

    public SerializableType getValue() {
        return this.value;
    }

    public Instant getTimestamp() {
        return this.timestamp;
    }

    @Override
    public String toString() {
        return "RegisterOperation{" +
                "opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                ", value=" + value +
                '}';
    }

    public static CRDTOpSerializer<RegisterOperation> serializer = new CRDTOpSerializer<RegisterOperation>() {
        @Override
        public void serialize(RegisterOperation registerOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            Operation.serialize(registerOperation, out);
            serializers[0].serialize(registerOperation.value, out);
            out.writeLong(registerOperation.timestamp.getEpochSecond());
            out.writeInt(registerOperation.timestamp.getNano());
        }

        @Override
        public RegisterOperation deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            int size = in.readInt();
            byte[] opType = new byte[size];
            in.readBytes(opType);
            size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            byte[] crdtType = new byte[size];
            in.readBytes(crdtType);
            SerializableType value = (SerializableType) serializers[0].deserialize(in);
            long epoch = in.readLong();
            int nano = in.readInt();
            return new RegisterOperation(new String(opType), new String(crdtId), new String(crdtType), value, Instant.ofEpochSecond(epoch, nano));
        }
    };

}
