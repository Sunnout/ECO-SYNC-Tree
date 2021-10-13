package protocols.broadcast.plumtree.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class ReversePruneMessage extends ProtoMessage {

    public static final short MSG_ID = 905;

    @Override
    public String toString() {
        return "ReversePruneMessage{}";
    }

    public ReversePruneMessage() {
        super(MSG_ID);
    }

    public static ISerializer<ReversePruneMessage> serializer = new ISerializer<ReversePruneMessage>() {
        @Override
        public void serialize(ReversePruneMessage reversePruneMessage, ByteBuf out) {

        }

        @Override
        public ReversePruneMessage deserialize(ByteBuf in) {
            return new ReversePruneMessage();
        }
    };
}
