package protocols.replication.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class SyncOperationMessage extends ProtoMessage {
    public static final short MSG_ID = 902;

    private final List<byte[]> ops;

    @Override
    public String toString() {
        return "SyncOperationMessage{" +
                "ops=" + ops +
                '}';
    }

    public SyncOperationMessage(List<byte[]> ops) {
        super(MSG_ID);
        this.ops = ops;
    }

    public List<byte[]> getOperations() {
        return ops;
    }

    public static ISerializer<SyncOperationMessage> serializer = new ISerializer<SyncOperationMessage>() {
        @Override
        public void serialize(SyncOperationMessage syncOperationMessage, ByteBuf out) throws IOException {
            out.writeInt(syncOperationMessage.ops.size());
            for(byte[] op : syncOperationMessage.ops) {
                out.writeInt(op.length);
                if (op.length > 0) {
                    out.writeBytes(op);
                }
            }
        }

        @Override
        public SyncOperationMessage deserialize(ByteBuf in) {
            int size = in.readInt();
            List<byte[]> ops = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                int len = in.readInt();
                byte[] op = new byte[len];
                if (size > 0)
                    in.readBytes(op);
                ops.add(op);
            }
            return new SyncOperationMessage(ops);
        }
    };
}
