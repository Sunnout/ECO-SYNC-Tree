package protocols.broadcast.common.messages;

import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class VectorClockMessage extends ProtoMessage {
    public static final short MSG_ID = 906;

    private final UUID mid;
    private final Host sender;
    private final VectorClock vc;

    @Override
    public String toString() {
        return "VectorClockMessage{" +
                "mid=" + mid +
                ", sender=" + sender +
                ", vc=" + vc +
                '}';
    }

    public VectorClockMessage(UUID mid, Host sender, VectorClock vc) {
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

    public static ISerializer<VectorClockMessage> serializer = new ISerializer<VectorClockMessage>() {
        @Override
        public void serialize(VectorClockMessage vectorClockMessage, ByteBuf out) throws IOException {
            out.writeLong(vectorClockMessage.mid.getMostSignificantBits());
            out.writeLong(vectorClockMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(vectorClockMessage.sender, out);
            VectorClock.serializer.serialize(vectorClockMessage.vc, out);
        }

        @Override
        public VectorClockMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            VectorClock vc = VectorClock.serializer.deserialize(in);
            return new VectorClockMessage(mid, sender, vc);
        }
    };
}
