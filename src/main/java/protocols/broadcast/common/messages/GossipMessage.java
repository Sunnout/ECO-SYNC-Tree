package protocols.broadcast.common.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class GossipMessage extends ProtoMessage {
    public static final short MSG_ID = 901;

    private final UUID mid;
    private final Host originalSender;
    private final int senderClock;
    private final byte[] content;

    @Override
    public String toString() {
        return "GossipMessage{" +
                "mid=" + mid +
                ", sender=" + originalSender +
                ", senderClock=" + senderClock +
                '}';
    }

    public GossipMessage(UUID mid, Host originalSender, int senderClock, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.originalSender = originalSender;
        this.senderClock = senderClock;
        this.content = content;
    }

	public Host getOriginalSender() {
        return originalSender;
    }

    public int getSenderClock() {
        return senderClock;
    }

    public UUID getMid() {
        return mid;
    }

    public byte[] getContent() {
        return content;
    }

    public static ISerializer<GossipMessage> serializer = new ISerializer<GossipMessage>() {
        @Override
        public void serialize(GossipMessage plumtreeGossipMessage, ByteBuf out) throws IOException {
            out.writeLong(plumtreeGossipMessage.mid.getMostSignificantBits());
            out.writeLong(plumtreeGossipMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(plumtreeGossipMessage.originalSender, out);
            out.writeInt(plumtreeGossipMessage.senderClock);
            out.writeInt(plumtreeGossipMessage.content.length);
            if (plumtreeGossipMessage.content.length > 0) {
                out.writeBytes(plumtreeGossipMessage.content);
            }
        }

        @Override
        public GossipMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            int senderClock = in.readInt();
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new GossipMessage(mid, sender, senderClock, content);
        }
    };

    public static GossipMessage deserialize(DataInputStream dis) throws IOException {
        long firstLong = dis.readLong();
        long secondLong = dis.readLong();
        UUID mid = new UUID(firstLong, secondLong);
        byte[] addrBytes = new byte[4];
        dis.read(addrBytes);
        int port = dis.readShort() & '\uffff';
        Host sender = new Host(InetAddress.getByAddress(addrBytes), port);
        int senderClock = dis.readInt();
        int size = dis.readInt();
        byte[] content = new byte[size];
        if (size > 0)
            dis.read(content);

        return new GossipMessage(mid, sender, senderClock, content);
    }
}
