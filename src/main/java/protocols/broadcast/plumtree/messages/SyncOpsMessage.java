package protocols.broadcast.plumtree.messages;

import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class SyncOpsMessage extends ProtoMessage {
    public static final short MSG_ID = 907;

    private final List<byte[]> ops;

    @Override
    public String toString() {
        return "SyncOpsMessage{" +
                "ops=" + ops +
                '}';
    }

    public SyncOpsMessage(List<byte[]> ops) {
        super(MSG_ID);
        this.ops = ops;
    }

    public List<byte[]> getOperations() {
        return ops;
    }

    public static ISerializer<SyncOpsMessage> serializer = new ISerializer<SyncOpsMessage>() {
        @Override
        public void serialize(SyncOpsMessage syncOpsMessage, ByteBuf out) {
            out.writeInt(syncOpsMessage.ops.size());
            for(byte[] op : syncOpsMessage.ops) {
                out.writeInt(op.length);
                if (op.length > 0) {
                    out.writeBytes(op);
                }
            }
        }

        @Override
        public SyncOpsMessage deserialize(ByteBuf in) {
            int size = in.readInt();
            List<byte[]> ops = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                int len = in.readInt();
                byte[] op = new byte[len];
                if (size > 0)
                    in.readBytes(op);
                ops.add(op);
            }
            return new SyncOpsMessage(ops);
        }
    };
}
