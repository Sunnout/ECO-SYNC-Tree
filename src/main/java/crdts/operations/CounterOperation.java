package crdts.operations;

import io.netty.buffer.ByteBuf;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;


public class CounterOperation extends Operation {

    private final int value;

    public CounterOperation(String opType, String crdtId, String crdtType, int value) {
        super(opType, crdtId, crdtType);
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return "CounterOperation{" +
                "opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                ", value=" + value +
                '}';
    }

    public static MyOpSerializer<CounterOperation> serializer = new MyOpSerializer<CounterOperation>() {
        @Override
        public void serialize(CounterOperation counterOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            Operation.serialize(counterOperation, out);
            out.writeInt(counterOperation.value);
        }

        @Override
        public CounterOperation deserialize(MySerializer[] serializers, ByteBuf in) {
            int size = in.readInt();
            byte[] opType = new byte[size];
            in.readBytes(opType);
            size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            byte[] crdtType = new byte[size];
            in.readBytes(crdtType);
            int value = in.readInt();
            return new CounterOperation(new String(opType), new String(crdtId), new String(crdtType), value);
        }
    };

}
