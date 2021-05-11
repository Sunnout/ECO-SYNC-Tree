package crdts.utils;

import datatypes.SerializableType;
import io.netty.buffer.ByteBuf;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.util.UUID;

public class TaggedElement {

    private final SerializableType value;
    private final UUID tag;

    public TaggedElement(SerializableType value, UUID tag) {
        this.value = value;
        this.tag = tag;
    }

    public SerializableType getValue() {
        return value;
    }

    public UUID getTag() {
        return tag;
    }

    public static MyOpSerializer<TaggedElement> serializer = new MyOpSerializer<TaggedElement>() {
        @Override
        public void serialize(TaggedElement taggedElement, MySerializer[] serializers, ByteBuf out) throws IOException {
            out.writeLong(taggedElement.tag.getMostSignificantBits());
            out.writeLong(taggedElement.tag.getLeastSignificantBits());
            serializers[0].serialize(taggedElement.value, out);
        }

        @Override
        public TaggedElement deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID tag = new UUID(firstLong, secondLong);
            SerializableType value = (SerializableType) serializers[0].deserialize(in);
            return new TaggedElement(value, tag);
        }
    };

    @Override
    public String toString() {
        return "TaggedElement{" +
                "value=" + value +
                ", tag=" + tag +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof TaggedElement))
            return false;

        TaggedElement te = (TaggedElement) o;
        return te.getValue().equals(this.value) && te.getTag().equals(this.tag);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode() + this.tag.hashCode();
    }

}
