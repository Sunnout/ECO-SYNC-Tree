package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import protocols.broadcast.common.utils.StateAndVC;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class SynchronizationMessage extends ProtoMessage {
    public static final short MSG_ID = 908;

    private final UUID mid;
    private final StateAndVC stateAndVC;
    private final List<byte[]> msgs;

    public SynchronizationMessage(UUID mid, StateAndVC stateAndVC, List<byte[]> msgs) {
        super(MSG_ID);
        this.mid = mid;
        this.stateAndVC = stateAndVC;
        this.msgs = msgs;
    }

    public UUID getMid() {
        return mid;
    }

    public StateAndVC getStateAndVC() {
        return stateAndVC;
    }

    public List<byte[]> getMsgs() {
        return msgs;
    }

    public static ISerializer<SynchronizationMessage> serializer = new ISerializer<SynchronizationMessage>() {
        @Override
        public void serialize(SynchronizationMessage synchronizationMessage, ByteBuf out) throws IOException {
            out.writeLong(synchronizationMessage.mid.getMostSignificantBits());
            out.writeLong(synchronizationMessage.mid.getLeastSignificantBits());
            boolean hasStateAndVC = synchronizationMessage.stateAndVC != null;
            out.writeBoolean(hasStateAndVC);
            if(hasStateAndVC)
                StateAndVC.serializer.serialize(synchronizationMessage.stateAndVC, out);
            out.writeInt(synchronizationMessage.msgs.size());
            for(byte[] msg : synchronizationMessage.msgs) {
                out.writeInt(msg.length);
                if (msg.length > 0) {
                    out.writeBytes(msg);
                }
            }
        }

        @Override
        public SynchronizationMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            boolean hasStateAndVC = in.readBoolean();
            StateAndVC stateAndVC = null;
            if(hasStateAndVC)
                stateAndVC = StateAndVC.serializer.deserialize(in);
            int size = in.readInt();
            List<byte[]> msgs = new LinkedList<>();
            for(int i = 0; i < size; i++) {
                int len = in.readInt();
                byte[] msg = new byte[len];
                if (size > 0)
                    in.readBytes(msg);
                msgs.add(msg);
            }
            return new SynchronizationMessage(mid, stateAndVC, msgs);
        }
    };

    @Override
    public String toString() {
        return "SynchronizationMessage{" +
                "mid=" + mid +
                ", nMsgs=" + msgs.size() +
                ", stateAndVC=" + stateAndVC +
                '}';
    }
}
