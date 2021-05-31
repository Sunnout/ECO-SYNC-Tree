package protocols.broadcast.plumtree.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.LinkedList;
import java.util.List;

public class SyncOpsMessage extends ProtoMessage {
    public static final short MSG_ID = 907;

    private final List<byte[]> ids;
    private final List<byte[]> ops;

    public SyncOpsMessage(List<byte[]> ids, List<byte[]> ops) {
        super(MSG_ID);
        this.ids = ids;
        this.ops = ops;
    }

    public List<byte[]> getIds() {
        return ids;
    }

    public List<byte[]> getOperations() {
        return ops;
    }

    public static ISerializer<SyncOpsMessage> serializer = new ISerializer<SyncOpsMessage>() {
        @Override
        public void serialize(SyncOpsMessage syncOpsMessage, ByteBuf out) {
            out.writeInt(syncOpsMessage.ids.size());
            for(byte[] id : syncOpsMessage.ids) {
                out.writeInt(id.length);
                if (id.length > 0) {
                    out.writeBytes(id);
                }
            }

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
            List<byte[]> ids = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                int len = in.readInt();
                byte[] id = new byte[len];
                if (size > 0)
                    in.readBytes(id);
                ids.add(id);
            }

            size = in.readInt();
            List<byte[]> ops = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                int len = in.readInt();
                byte[] op = new byte[len];
                if (size > 0)
                    in.readBytes(op);
                ops.add(op);
            }
            return new SyncOpsMessage(ids, ops);
        }
    };

    @Override
    public String toString() {
        return "SyncOpsMessage{" +
                "nIds=" + ids.size() +
                ", nOps=" + ops.size() +
                '}';
    }
}
