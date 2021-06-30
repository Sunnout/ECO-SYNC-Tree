package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class SendVectorClockMessage extends ProtoMessage {
    public static final short MSG_ID = 908;

    private final UUID mid;
    private final Host sender;

    @Override
    public String toString() {
        return "SendVectorClockMessage{" +
                "mid=" + mid + ", " +
                "sender=" + sender +
                '}';
    }

    public SendVectorClockMessage(UUID mid, Host sender) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
    }

	public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }


    public static ISerializer<SendVectorClockMessage> serializer = new ISerializer<SendVectorClockMessage>() {
        @Override
        public void serialize(SendVectorClockMessage vectorClockMessage, ByteBuf out) throws IOException {
            out.writeLong(vectorClockMessage.mid.getMostSignificantBits());
            out.writeLong(vectorClockMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(vectorClockMessage.sender, out);
        }

        @Override
        public SendVectorClockMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            return new SendVectorClockMessage(mid, sender);
        }
    };
}
