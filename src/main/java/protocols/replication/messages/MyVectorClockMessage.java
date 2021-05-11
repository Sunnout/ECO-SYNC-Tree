package protocols.replication.messages;

import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class MyVectorClockMessage extends ProtoMessage {
    public static final short MSG_ID = 901;

    private final VectorClock vc;

    @Override
    public String toString() {
        return "MyVectorClockMessage{" +
                "vc=" + vc +
                '}';
    }

    public MyVectorClockMessage(VectorClock vc) {
        super(MSG_ID);
        this.vc = vc;
    }

    public VectorClock getVectorClock() {
        return vc;
    }

    public static ISerializer<MyVectorClockMessage> serializer = new ISerializer<MyVectorClockMessage>() {
        @Override
        public void serialize(MyVectorClockMessage vectorClockMessage, ByteBuf out) throws IOException {
            VectorClock.serializer.serialize(vectorClockMessage.vc, out);
        }

        @Override
        public MyVectorClockMessage deserialize(ByteBuf in) throws IOException {
            VectorClock vc = VectorClock.serializer.deserialize(in);
            return new MyVectorClockMessage(vc);
        }
    };
}