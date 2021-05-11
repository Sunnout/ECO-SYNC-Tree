package datatypes;

import io.netty.buffer.ByteBuf;
import serializers.MySerializer;


public class FloatType extends SerializableType{

    private final Float value;

    public FloatType(Float value) {
        this.value = value;
    }

    public static MySerializer<FloatType> serializer = new MySerializer<FloatType>() {
        @Override
        public void serialize(FloatType floatType, ByteBuf out) {
            out.writeFloat(floatType.value);
        }

        @Override
        public FloatType deserialize(ByteBuf in) {
            return new FloatType(in.readFloat());
        }
    };

    public Float getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof FloatType))
            return false;

        return ((FloatType) o).getValue().equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }
}
