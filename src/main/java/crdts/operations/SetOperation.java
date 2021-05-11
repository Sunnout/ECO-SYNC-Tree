package crdts.operations;

import crdts.utils.TaggedElement;
import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class SetOperation extends Operation {

    private Set<TaggedElement> set;

    public SetOperation(Host sender, int senderClock, VectorClock vc, String opType, String crdtId, String crdtType, Set<TaggedElement> set) {
        super(sender, senderClock, vc, opType, crdtId, crdtType);
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

    public static MyOpSerializer<SetOperation> serializer = new MyOpSerializer<SetOperation>() {
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
            VectorClock vc = VectorClock.serializer.deserialize(in);
            Host sender = Host.serializer.deserialize(in);
            int senderClock = in.readInt();
            size = in.readInt();
            for(int i = 0; i < size; i++) {
                set.add(TaggedElement.serializer.deserialize(serializers, in));
            }
            return new SetOperation(sender, senderClock, vc, new String(opType), new String(crdtId), new String(crdtType), set);
        }
    };

}
