package crdts.operations.vc;

import crdts.operations.Operation;
import crdts.operations.SetOperation;
import crdts.utils.TaggedElement;
import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SetOperationVC extends OperationVC {

    private Set<TaggedElement> set;

    public SetOperationVC(Host sender, int senderClock, String opType, String crdtId, String crdtType, Set<TaggedElement> set, VectorClock vc) {
        super(sender, senderClock, opType, crdtId, crdtType, vc);
        this.set = set;
    }

    public Set<TaggedElement> getSet() {
        return this.set;
    }

    @Override
    public String toString() {
        return "SetOperationVC{" +
                "sender=" + sender +
                ", senderClock=" + senderClock +
                ", opType=" + opType +
                ", crdtId=" + crdtId +
                ", crdtType=" + crdtType +
                ", set=" + set +
                '}';
    }

    public static MyOpSerializer<SetOperationVC> serializer = new MyOpSerializer<SetOperationVC>() {
        @Override
        public void serialize(SetOperationVC setOperation, MySerializer[] serializers, ByteBuf out) throws IOException {
            OperationVC.serialize(setOperation, out);
            out.writeInt(setOperation.set.size());
            for (TaggedElement e : setOperation.set) {
                TaggedElement.serializer.serialize(e, serializers, out);
            }
        }

        @Override
        public SetOperationVC deserialize(MySerializer[] serializers, ByteBuf in) throws IOException {
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
            return new SetOperationVC(sender, senderClock, new String(opType), new String(crdtId), new String(crdtType), set, vc);
        }
    };

}
