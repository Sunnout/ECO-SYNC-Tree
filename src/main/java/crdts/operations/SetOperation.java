package crdts.operations;

import crdts.utils.TaggedElement;
import io.netty.buffer.ByteBuf;
import serializers.CRDTOpSerializer;
import serializers.MySerializer;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class SetOperation extends Operation {

    private Set<TaggedElement> set;

    public SetOperation(String opType, String crdtId, String crdtType, Set<TaggedElement> set) {
        super(opType, crdtId, crdtType);
        this.set = set;
    }

    public Set<TaggedElement> getSet() {
        return this.set;
    }

    @Override
    public String toString() {
        return "SetOperation{" +
                "opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                ", set=" + set +
                '}';
    }

    public static CRDTOpSerializer<SetOperation> serializer = new CRDTOpSerializer<SetOperation>() {
        @Override
        public void serialize(SetOperation setOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            Operation.serialize(setOperation, out);
            out.writeInt(setOperation.set.size());
            for (TaggedElement e : setOperation.set) {
                TaggedElement.serializer.serialize(e, serializers, out);
            }
        }

        @Override
        public SetOperation deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
            Set<TaggedElement> set = new HashSet<>();
            int size = in.readInt();
            byte[] opType = new byte[size];
            in.readBytes(opType);
            size = in.readInt();
            byte[] crdtId = new byte[size];
            in.readBytes(crdtId);
            size = in.readInt();
            byte[] crdtType = new byte[size];
            in.readBytes(crdtType);
            size = in.readInt();
            for(int i = 0; i < size; i++) {
                set.add(TaggedElement.serializer.deserialize(serializers, in));
            }
            return new SetOperation(new String(opType), new String(crdtId), new String(crdtType), set);
        }
    };

}
