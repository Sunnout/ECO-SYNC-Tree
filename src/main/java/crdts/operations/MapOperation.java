package crdts.operations;

import crdts.utils.TaggedElement;
import crdts.utils.VectorClock;
import datatypes.SerializableType;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;


public class MapOperation extends Operation {

    private final TaggedElement elem;
    private final SerializableType key;
    private final Set<TaggedElement> set;
    

    public MapOperation(Host sender, int senderClock, String opType, String crdtId, String crdtType, SerializableType key, TaggedElement elem, Set<TaggedElement> set) {
        super(sender, senderClock, opType, crdtId, crdtType);
        this.elem = elem;
        this.key = key;
        this.set = set;
    }

    public Set<TaggedElement> getSet() {
        return this.set;
    }

    public TaggedElement getElement() {
        return this.elem;
    }

    public SerializableType getKey() {
        return this.key;
    }

    @Override
    public String toString() {
        return "MapOperation{" +
                "opType=" + opType +
                ", crdtId=" + crdtId +
                ", key=" + key +
                ", elem=" + elem +
                ", set=" + set +
                '}';
    }

    public static MyOpSerializer<MapOperation> serializer = new MyOpSerializer<MapOperation>() {
        @Override
        public void serialize(MapOperation mapOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            MySerializer[] teSerializer = getValueSerializer(serializers);

            Operation.serialize(mapOperation, out);
            serializers[0].serialize(mapOperation.key, out);
            boolean isNull = mapOperation.elem == null;
            out.writeBoolean(isNull);
            if(!isNull) {
                TaggedElement.serializer.serialize(mapOperation.elem, teSerializer, out);
            }
            out.writeInt(mapOperation.set.size());
            for (TaggedElement taggedElement : mapOperation.set) {
                TaggedElement.serializer.serialize(taggedElement, teSerializer, out);
            }
        }

        @Override
        public MapOperation deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            MySerializer[] teSerializer = getValueSerializer(serializers);

            Set set = new HashSet<>();
            int size = in.readInt();
            byte[] opType = new byte[size];
            in.readBytes(opType);
            size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            byte[] crdtType = new byte[size];
            in.readBytes(crdtType);
            Host sender = Host.serializer.deserialize(in);
            int senderClock = in.readInt();
            SerializableType key = (SerializableType) serializers[0].deserialize(in);
            boolean isNull = in.readBoolean();
            TaggedElement elem = null;
            if(!isNull) {
                elem = TaggedElement.serializer.deserialize(teSerializer, in);
            }
            size = in.readInt();
            for(int i = 0; i < size; i++) {
                set.add(TaggedElement.serializer.deserialize(teSerializer, in));
            }
            return new MapOperation(sender, senderClock, new String(opType), new String(crdtId), new String(crdtType), key, elem, set);
        }
    };

    private static MySerializer[] getValueSerializer(MySerializer[] serializers) {
        MySerializer[] array = new MySerializer[1];
        array[0] = serializers[1];
        return array;
    }

}
