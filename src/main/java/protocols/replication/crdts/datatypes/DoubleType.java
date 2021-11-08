package protocols.replication.crdts.datatypes;

import io.netty.buffer.ByteBuf;
import protocols.replication.crdts.serializers.MySerializer;


public class DoubleType extends SerializableType{

    private final Double value;

    public DoubleType(Double value) {
        this.value = value;

    }

    public static MySerializer<DoubleType> serializer = new MySerializer<DoubleType>() {
        @Override
        public void serialize(DoubleType doubleType, ByteBuf out) {
            out.writeDouble(doubleType.value);
        }

        @Override
        public DoubleType deserialize(ByteBuf in) {
            return new DoubleType(in.readDouble());
        }
    };

    public Double getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof DoubleType))
            return false;

        return ((DoubleType) o).getValue().equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public int compareTo(Object o) {
        return this.value.compareTo(((DoubleType)o).value);
    }
}
