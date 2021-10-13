package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class SyncOpsMessage extends ProtoMessage {
    public static final short MSG_ID = 908;

    private final UUID mid;
    private final List<byte[]> msgs;

    public SyncOpsMessage(UUID mid, List<byte[]> msgs) {
        super(MSG_ID);
        this.mid = mid;
        this.msgs = msgs;
    }

    public UUID getMid() {
        return mid;
    }

    public List<byte[]> getMsgs() {
        return msgs;
    }

    public static ISerializer<SyncOpsMessage> serializer = new ISerializer<SyncOpsMessage>() {
        @Override
        public void serialize(SyncOpsMessage syncOpsMessage, ByteBuf out) {
            out.writeLong(syncOpsMessage.mid.getMostSignificantBits());
            out.writeLong(syncOpsMessage.mid.getLeastSignificantBits());
            out.writeInt(syncOpsMessage.msgs.size());
            for(byte[] msg : syncOpsMessage.msgs) {
                out.writeInt(msg.length);
                if (msg.length > 0) {
                    out.writeBytes(msg);
                }
            }
        }

        @Override
        public SyncOpsMessage deserialize(ByteBuf in) {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            int size = in.readInt();
            List<byte[]> msgs = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                int len = in.readInt();
                byte[] msg = new byte[len];
                if (size > 0)
                    in.readBytes(msg);
                msgs.add(msg);
            }
            return new SyncOpsMessage(mid, msgs);
        }
    };

    @Override
    public String toString() {
        return "SyncOpsMessage{" +
                "mid=" + mid +
                ", nMsgs=" + msgs.size() +
                '}';
    }
}
