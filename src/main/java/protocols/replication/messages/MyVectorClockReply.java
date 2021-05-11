package protocols.replication.messages;

import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class MyVectorClockReply extends ProtoMessage {
    public static final short MSG_ID = 903;

    private final VectorClock vc;

    @Override
    public String toString() {
        return "MyVectorClockReply{" +
                "vc=" + vc +
                '}';
    }

    public MyVectorClockReply(VectorClock vc) {
        super(MSG_ID);
        this.vc = vc;
    }

    public VectorClock getVectorClock() {
        return vc;
    }

    public static ISerializer<MyVectorClockReply> serializer = new ISerializer<MyVectorClockReply>() {
        @Override
        public void serialize(MyVectorClockReply vectorClockMessage, ByteBuf out) throws IOException {
            VectorClock.serializer.serialize(vectorClockMessage.vc, out);
        }

        @Override
        public MyVectorClockReply deserialize(ByteBuf in) throws IOException {
            VectorClock vc = VectorClock.serializer.deserialize(in);
            return new MyVectorClockReply(vc);
        }
    };
}
