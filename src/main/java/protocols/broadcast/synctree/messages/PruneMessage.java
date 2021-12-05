package protocols.broadcast.synctree.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PruneMessage extends ProtoMessage {

    public static final short MSG_ID = 904;

    @Override
    public String toString() {
        return "PruneMessage{}";
    }

    public PruneMessage() {
        super(MSG_ID);
    }

    public static ISerializer<PruneMessage> serializer = new ISerializer<PruneMessage>() {
        @Override
        public void serialize(PruneMessage pruneMessage, ByteBuf out) {

        }

        @Override
        public PruneMessage deserialize(ByteBuf in) {
            return new PruneMessage();
        }
    };
}
