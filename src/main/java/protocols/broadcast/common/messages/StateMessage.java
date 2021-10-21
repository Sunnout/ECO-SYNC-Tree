package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class StateMessage extends ProtoMessage {
    public static final short MSG_ID = 910;

    private final UUID mid;
    private final Host sender;
    private final byte[] state;

    @Override
    public String toString() {
        return "StateMessage{" +
                "mid=" + mid +
                ", sender=" + sender +
                '}';
    }

    public StateMessage(UUID mid, Host sender, byte[] state) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.state = state;
    }

	public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public byte[] getState() {
        return state;
    }

    public static ISerializer<StateMessage> serializer = new ISerializer<StateMessage>() {
        @Override
        public void serialize(StateMessage stateMessage, ByteBuf out) throws IOException {
            out.writeLong(stateMessage.mid.getMostSignificantBits());
            out.writeLong(stateMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(stateMessage.sender, out);
            out.writeInt(stateMessage.state.length);
            out.writeBytes(stateMessage.getState());
        }

        @Override
        public StateMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            int size = in.readInt();
            byte[] state = new byte[size];
            in.readBytes(state);
            return new StateMessage(mid, sender, state);
        }
    };
}
