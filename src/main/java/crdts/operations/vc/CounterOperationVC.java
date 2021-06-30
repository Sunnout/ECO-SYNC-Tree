package crdts.operations.vc;

import crdts.operations.Operation;
import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;


public class CounterOperationVC extends OperationVC {

    private final int value;

    public CounterOperationVC(Host sender, int senderClock, String opType, String crdtId, String crdtType, int value, VectorClock vc) {
        super(sender, senderClock, opType, crdtId, crdtType, vc);
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return "CounterOperationVC{" +
                "sender=" + sender +
                ", senderClock=" + senderClock +
                ", opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                ", value=" + value +
                '}';
    }

    public static MyOpSerializer<CounterOperationVC> serializer = new MyOpSerializer<CounterOperationVC>() {
        @Override
        public void serialize(CounterOperationVC counterOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            OperationVC.serialize(counterOperation, out);
            out.writeInt(counterOperation.value);
        }

        @Override
        public CounterOperationVC deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
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
            int value = in.readInt();
            return new CounterOperationVC(sender, senderClock, new String(opType), new String(crdtId), new String(crdtType), value, vc);
        }
    };

}
