package protocols.broadcast.common.messages;

import protocols.broadcast.common.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class VectorClockMessage extends ProtoMessage {
    public static final short MSG_ID = 909;

    private final UUID mid;
    private final VectorClock vc;

    @Override
    public String toString() {
        return "VectorClockMessage{" +
                "mid=" + mid +
                ", vc=" + vc +
                '}';
    }

    public VectorClockMessage(UUID mid, VectorClock vc) {
        super(MSG_ID);
        this.mid = mid;
        this.vc = vc;
    }

    public UUID getMid() {
        return mid;
    }

    public VectorClock getVectorClock() {
        return vc;
    }

    public static ISerializer<VectorClockMessage> serializer = new ISerializer<VectorClockMessage>() {
        @Override
        public void serialize(VectorClockMessage vectorClockMessage, ByteBuf out) throws IOException {
            out.writeLong(vectorClockMessage.mid.getMostSignificantBits());
            out.writeLong(vectorClockMessage.mid.getLeastSignificantBits());
            VectorClock.serializer.serialize(vectorClockMessage.vc, out);
        }

        @Override
        public VectorClockMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            VectorClock vc = VectorClock.serializer.deserialize(in);
            return new VectorClockMessage(mid, vc);
        }
    };
}
