package datatypes;

import io.netty.buffer.ByteBuf;
import serializers.MySerializer;


public class LongType extends SerializableType{

    private final Long value;

    public LongType(Long value) {
        this.value = value;
    }

    public static MySerializer<LongType> serializer = new MySerializer<LongType>() {
        @Override
        public void serialize(LongType longType, ByteBuf out) {
            out.writeLong(longType.value);
        }

        @Override
        public LongType deserialize(ByteBuf in) {
            return new LongType(in.readLong());
        }
    };

    public Long getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof LongType))
            return false;

        return ((LongType) o).getValue().equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public int compareTo(Object o) {
        return this.value.compareTo(((LongType)o).value);
    }
}
