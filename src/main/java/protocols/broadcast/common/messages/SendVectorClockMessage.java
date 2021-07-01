package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class SendVectorClockMessage extends ProtoMessage {
    public static final short MSG_ID = 908;

    private final UUID mid;

    @Override
    public String toString() {
        return "SendVectorClockMessage{" +
                "mid=" + mid +
                '}';
    }

    public SendVectorClockMessage(UUID mid) {
        super(MSG_ID);
        this.mid = mid;
    }

    public UUID getMid() {
        return mid;
    }


    public static ISerializer<SendVectorClockMessage> serializer = new ISerializer<SendVectorClockMessage>() {
        @Override
        public void serialize(SendVectorClockMessage vectorClockMessage, ByteBuf out) {
            out.writeLong(vectorClockMessage.mid.getMostSignificantBits());
            out.writeLong(vectorClockMessage.mid.getLeastSignificantBits());
        }

        @Override
        public SendVectorClockMessage deserialize(ByteBuf in) {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            return new SendVectorClockMessage(mid);
        }
    };
}
