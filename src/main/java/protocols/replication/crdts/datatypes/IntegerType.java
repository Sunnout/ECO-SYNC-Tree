package protocols.replication.crdts.datatypes;

import io.netty.buffer.ByteBuf;
import protocols.replication.crdts.serializers.MySerializer;


public class IntegerType extends SerializableType {

    private final Integer value;

    public IntegerType(Integer value) {
        this.value = value;
    }

    public static MySerializer<IntegerType> serializer = new MySerializer<IntegerType>() {
        @Override
        public void serialize(IntegerType integerType, ByteBuf out) {
            out.writeInt(integerType.value);
        }

        @Override
        public IntegerType deserialize(ByteBuf in) {
            return new IntegerType(in.readInt());
        }
    };

    public Integer getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof IntegerType))
            return false;

        return ((IntegerType) o).getValue().equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public int compareTo(Object o) {
        return this.value.compareTo(((IntegerType)o).value);
    }
}
