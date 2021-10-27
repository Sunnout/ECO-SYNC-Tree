package datatypes;

import io.netty.buffer.ByteBuf;
import serializers.MySerializer;


public class ByteType extends SerializableType{

    private final Byte value;

    public ByteType(Byte value) {
        this.value = value;

    }

    public static MySerializer<ByteType> serializer = new MySerializer<ByteType>() {
        @Override
        public void serialize(ByteType byteType, ByteBuf out) {
            out.writeByte(byteType.value);
        }

        @Override
        public ByteType deserialize(ByteBuf in) {
            return new ByteType(in.readByte());
        }
    };

    public Byte getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof ByteType))
            return false;

        return ((ByteType) o).getValue().equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public int compareTo(Object o) {
        return this.value.compareTo(((ByteType)o).value);
    }
}
