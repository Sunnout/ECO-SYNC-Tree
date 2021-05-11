package protocols.replication.messages;

import crdts.operations.Operation;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.List;

public class SyncOperationsMessage extends ProtoMessage {
    public static final short MSG_ID = 902;

    private final List<Operation> ops;

    @Override
    public String toString() {
        return "SyncOperationsMessage{" +
                "ops=" + ops +
                '}';
    }

    public SyncOperationsMessage(List<Operation> ops) {
        super(MSG_ID);
        this.ops = ops;
    }

    public List<Operation> getOperations() {
        return ops;
    }

    public static ISerializer<SyncOperationsMessage> serializer = new ISerializer<SyncOperationsMessage>() {
        @Override
        public void serialize(SyncOperationsMessage syncOperationsMessage, ByteBuf out) {
            out.writeInt(syncOperationsMessage.ops.size());
            for(Operation op : syncOperationsMessage.ops) {
                //TODO: how to serialize ops??
            }
        }

        @Override
        public SyncOperationsMessage deserialize(ByteBuf in) {
            int size = in.readInt();
            for(int i = 0; i < size; i++) {
                //TODO: deserialize ops
            }
            return new SyncOperationsMessage(null);
        }
    };
}
