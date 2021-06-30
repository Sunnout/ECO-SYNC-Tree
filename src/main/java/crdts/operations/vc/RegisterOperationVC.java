package crdts.operations.vc;

import crdts.utils.VectorClock;
import datatypes.SerializableType;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.time.Instant;


public class RegisterOperationVC extends OperationVC {

    private final SerializableType value;
    private final Instant timestamp;

    public RegisterOperationVC(Host sender, int senderClock, String opType, String crdtId, String crdtType, SerializableType value, Instant timestamp, VectorClock vc) {
        super(sender, senderClock, opType, crdtId, crdtType, vc);
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
        return "RegisterOperationVC{" +
                "sender=" + sender +
                ", senderClock=" + senderClock +
                ", opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                ", value=" + value +
                '}';
    }

    public static MyOpSerializer<RegisterOperationVC> serializer = new MyOpSerializer<RegisterOperationVC>() {
        @Override
        public void serialize(RegisterOperationVC registerOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            OperationVC.serialize(registerOperation, out);
            serializers[0].serialize(registerOperation.value, out);
            out.writeLong(registerOperation.timestamp.getEpochSecond());
            out.writeInt(registerOperation.timestamp.getNano());
        }

        @Override
        public RegisterOperationVC deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
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
            return new RegisterOperationVC(sender, senderClock, new String(opType), new String(crdtId), new String(crdtType), value, Instant.ofEpochSecond(epoch, nano), vc);
        }
    };

}
