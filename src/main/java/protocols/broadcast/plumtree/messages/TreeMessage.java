package protocols.broadcast.plumtree.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class TreeMessage extends ProtoMessage {
    public static final short MSG_ID = 905;

    private final UUID mid;
    private final Host sender;

    @Override
    public String toString() {
        return "TreeMessage{" +
                "mid=" + mid + ", " +
                "sender=" + sender +
                '}';
    }

    public TreeMessage(UUID mid, Host sender) {
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


    public static ISerializer<TreeMessage> serializer = new ISerializer<TreeMessage>() {
        @Override
        public void serialize(TreeMessage treeMessage, ByteBuf out) throws IOException {
            out.writeLong(treeMessage.mid.getMostSignificantBits());
            out.writeLong(treeMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(treeMessage.sender, out);
        }

        @Override
        public TreeMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            return new TreeMessage(mid, sender);
        }
    };
}