package protocols.broadcast.plumtree.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class AddPendingToEagerMessage extends ProtoMessage {
    public static final short MSG_ID = 909;

    private final UUID mid;
    private final Host pending;

    @Override
    public String toString() {
        return "AddPendingToEagerMessage{" +
                "mid=" + mid + ", " +
                "pending=" + pending +
                '}';
    }

    public AddPendingToEagerMessage(UUID mid, Host pending) {
        super(MSG_ID);
        this.mid = mid;
        this.pending = pending;
    }

	public Host getPending() {
        return pending;
    }

    public UUID getMid() {
        return mid;
    }


    public static ISerializer<AddPendingToEagerMessage> serializer = new ISerializer<AddPendingToEagerMessage>() {
        @Override
        public void serialize(AddPendingToEagerMessage vectorClockMessage, ByteBuf out) throws IOException {
            out.writeLong(vectorClockMessage.mid.getMostSignificantBits());
            out.writeLong(vectorClockMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(vectorClockMessage.pending, out);
        }

        @Override
        public AddPendingToEagerMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host pending = Host.serializer.deserialize(in);
            return new AddPendingToEagerMessage(mid, pending);
        }
    };
}
