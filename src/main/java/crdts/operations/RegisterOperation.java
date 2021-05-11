package crdts.operations;

import crdts.utils.VectorClock;
import datatypes.SerializableType;
import io.netty.buffer.ByteBuf;

import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.time.Instant;


public class RegisterOperation extends Operation {

    private final SerializableType value;
    private final Instant timestamp;

    public RegisterOperation(Host sender, int senderClock, VectorClock vc, String opType, String crdtId, String crdtType, SerializableType value, Instant timestamp) {
        super(sender, senderClock, vc, opType, crdtId, crdtType);
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

    public static MyOpSerializer<RegisterOperation> serializer = new MyOpSerializer<RegisterOperation>() {
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
            VectorClock vc = VectorClock.serializer.deserialize(in);
            Host sender = Host.serializer.deserialize(in);
            int senderClock = in.readInt();
            SerializableType value = (SerializableType) serializers[0].deserialize(in);
            long epoch = in.readLong();
            int nano = in.readInt();
            return new RegisterOperation(sender, senderClock, vc, new String(opType), new String(crdtId), new String(crdtType), value, Instant.ofEpochSecond(epoch, nano));
        }
    };

}
