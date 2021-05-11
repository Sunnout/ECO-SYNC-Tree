package datatypes;

import io.netty.buffer.ByteBuf;
import serializers.MySerializer;


public class BooleanType extends SerializableType{

    private final Boolean value;

    public BooleanType(Boolean value) {
        this.value = value;
    }

    public static MySerializer<BooleanType> serializer = new MySerializer<BooleanType>() {
        @Override
        public void serialize(BooleanType booleanType, ByteBuf out) {
            out.writeBoolean(booleanType.value);
        }

        @Override
        public BooleanType deserialize(ByteBuf in) {
            return new BooleanType(in.readBoolean());
        }
    };

    public Boolean getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof BooleanType))
            return false;

        return ((BooleanType) o).getValue() == this.value;
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

}
