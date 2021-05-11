package protocols.broadcast.plumtree.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.util.UUID;

public class GraftMessage extends ProtoMessage {

    public static final short MSG_ID = 902;

    private final UUID mid;
    private final int round;

    @Override
    public String toString() {
        return "GraftMessage{" +
                "mid=" + mid +
                ", round=" + round +
                '}';
    }

    public GraftMessage(UUID mid, int round) {
        super(MSG_ID);
        this.mid = mid;
        this.round = round;
    }

    public UUID getMid() {
        return mid;
    }

    public int getRound() {
        return round;
    }

    public static ISerializer<GraftMessage> serializer = new ISerializer<GraftMessage>() {
        @Override
        public void serialize(GraftMessage graftMessage, ByteBuf out) throws IOException {
            out.writeLong(graftMessage.mid.getMostSignificantBits());
            out.writeLong(graftMessage.mid.getLeastSignificantBits());
            out.writeInt(graftMessage.round);
        }

        @Override
        public GraftMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            return new GraftMessage(mid, in.readInt());
        }
    };
}
