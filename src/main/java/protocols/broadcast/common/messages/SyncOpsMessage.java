package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class SyncOpsMessage extends ProtoMessage {
    public static final short MSG_ID = 907;

    private final UUID mid;
    private final List<byte[]> ids;
    private final List<byte[]> ops;

    public SyncOpsMessage(UUID mid, List<byte[]> ids, List<byte[]> ops) {
        super(MSG_ID);
        this.mid = mid;
        this.ids = ids;
        this.ops = ops;
    }

    public UUID getMid() {
        return mid;
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
            out.writeLong(syncOpsMessage.mid.getMostSignificantBits());
            out.writeLong(syncOpsMessage.mid.getLeastSignificantBits());
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
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
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
            return new SyncOpsMessage(mid, ids, ops);
        }
    };

    @Override
    public String toString() {
        return "SyncOpsMessage{" +
                "mid=" + mid +
                ", nIds=" + ids.size() +
                ", nOps=" + ops.size() +
                '}';
    }
}
