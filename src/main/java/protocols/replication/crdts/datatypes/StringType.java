package protocols.replication.crdts.datatypes;

import io.netty.buffer.ByteBuf;
import protocols.replication.crdts.serializers.MySerializer;


public class StringType extends SerializableType{

    private final String value;

    public StringType(String value) {
        this.value = value;
    }

    public static MySerializer<StringType> serializer = new MySerializer<StringType>() {
        @Override
        public void serialize(StringType stringType, ByteBuf out) {
            byte[] data = stringType.value.getBytes();
            int size = data.length;
            out.writeInt(size);
            out.writeBytes(data);
        }

        @Override
        public StringType deserialize(ByteBuf in) {
            int size = in.readInt();
            byte[] string = new byte[size];
            in.readBytes(string);
            return new StringType(new String(string));
        }
    };

    public String getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof StringType))
            return false;

        return ((StringType) o).getValue().equals(this.value);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public int compareTo(Object o) {
        return this.value.compareTo(((StringType)o).value);
    }
}
