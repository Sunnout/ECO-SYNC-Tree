package protocols.broadcast.plumtree.messages;

import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class OriginalVectorClockMessage extends ProtoMessage {
    public static final short MSG_ID = 905;

    private final UUID mid;
    private final Host sender;
    private final VectorClock vc;

    @Override
    public String toString() {
        return "OriginalVectorClockMessage{" +
                "mid=" + mid +
                "sender=" + sender +
                "vc=" + vc +
                '}';
    }

    public OriginalVectorClockMessage(UUID mid, Host sender, VectorClock vc) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.vc = vc;
    }

	public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public VectorClock getVectorClock() {
        return vc;
    }


    public static ISerializer<OriginalVectorClockMessage> serializer = new ISerializer<OriginalVectorClockMessage>() {
        @Override
        public void serialize(OriginalVectorClockMessage originalVectorClockMessage, ByteBuf out) throws IOException {
            out.writeLong(originalVectorClockMessage.mid.getMostSignificantBits());
            out.writeLong(originalVectorClockMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(originalVectorClockMessage.sender, out);
            VectorClock.serializer.serialize(originalVectorClockMessage.vc, out);
        }

        @Override
        public OriginalVectorClockMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            VectorClock vc = VectorClock.serializer.deserialize(in);
            return new OriginalVectorClockMessage(mid, sender, vc);
        }
    };
}
