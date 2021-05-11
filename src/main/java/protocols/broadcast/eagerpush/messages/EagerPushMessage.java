package protocols.broadcast.eagerpush.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class EagerPushMessage extends ProtoMessage {
    public static final short MSG_ID = 501;

    private final UUID mid;
    private final Host sender;
    private final short toDeliver;
    private final byte[] content;

    @Override
    public String toString() {
        return "EagerPushMessage{" +
                "mid=" + mid +
                '}';
    }

    public EagerPushMessage(UUID mid, Host sender, short toDeliver, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.toDeliver = toDeliver;
        this.content = content;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public byte[] getContent() {
        return content;
    }

    public static ISerializer<EagerPushMessage> serializer = new ISerializer<EagerPushMessage>() {
        @Override
        public void serialize(EagerPushMessage eagerPushMessage, ByteBuf out) throws IOException {
            out.writeLong(eagerPushMessage.mid.getMostSignificantBits());
            out.writeLong(eagerPushMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(eagerPushMessage.sender, out);
            out.writeShort(eagerPushMessage.toDeliver);
            out.writeInt(eagerPushMessage.content.length);
            if (eagerPushMessage.content.length > 0) {
                out.writeBytes(eagerPushMessage.content);
            }
        }

        @Override
        public EagerPushMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new EagerPushMessage(mid, sender, toDeliver, content);
        }
    };
}
