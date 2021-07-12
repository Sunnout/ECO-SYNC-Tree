package protocols.broadcast.plumtree.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.UUID;

public class IHaveMessage extends ProtoMessage {

    public static final short MSG_ID = 903;

    private final UUID mid;

    @Override
    public String toString() {
        return "IHaveMessage{" +
                "mid=" + mid +
                '}';
    }

    public IHaveMessage(UUID mid) {
        super(MSG_ID);
        this.mid = mid;
    }

    public UUID getMid() {
        return mid;
    }

    public static ISerializer<IHaveMessage> serializer = new ISerializer<IHaveMessage>() {
        @Override
        public void serialize(IHaveMessage iHaveMessage, ByteBuf out) throws IOException {
            out.writeLong(iHaveMessage.mid.getMostSignificantBits());
            out.writeLong(iHaveMessage.mid.getLeastSignificantBits());
        }

        @Override
        public IHaveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            return new IHaveMessage(mid);
        }
    };
}
