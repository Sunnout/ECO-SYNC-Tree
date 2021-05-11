package protocols.broadcast.plumtree.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class GossipMessage extends ProtoMessage {
    public static final short MSG_ID = 901;

    private final UUID mid;
    private final Host sender;
    private final byte[] content;
    private int round;

    @Override
    public String toString() {
        return "PlumtreeGossipMessage{" +
                "mid=" + mid +
                '}';
    }

    public GossipMessage(UUID mid, Host sender, int round, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.round = round;
        this.content = content;
    }

    public int getRound() {
		return round;
	}

    public GossipMessage setRound(int round) {
        this.round = round;
        return this;
    }
    
    public void incrementRound() {
		this.round++;
	}


	public Host getSender() {
        return sender;
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
            Host.serializer.serialize(plumtreeGossipMessage.sender, out);
            out.writeInt(plumtreeGossipMessage.round);
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
            int round = in.readInt();
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new GossipMessage(mid, sender, round, content);
        }
    };
}
