package datatypes;

import io.netty.buffer.ByteBuf;
import serializers.MySerializer;


public class ShortType extends SerializableType{

    private final Short value;

    public ShortType(Short value) {
        this.value = value;
    }

    public static MySerializer<ShortType> serializer = new MySerializer<ShortType>() {
        @Override
        public void serialize(ShortType shortType, ByteBuf out) {
            out.writeShort(shortType.value);
        }

        @Override
        public ShortType deserialize(ByteBuf in) {
            return new ShortType(in.readShort());
        }
    };

    public Short getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof ShortType))
            return false;

        return ((ShortType) o).getValue().equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public int compareTo(Object o) {
        return this.value.compareTo(((ShortType)o).value);
    }
}
