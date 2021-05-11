package protocols.replication.messages;

import crdts.utils.VectorClock;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class MyVectorClockMessage extends ProtoMessage {
    public static final short MSG_ID = 901;

    private final Host sender;
    private final VectorClock vc;

    @Override
    public String toString() {
        return "MyVectorClockMessage{" +
                "sender=" + sender +
                "vc=" + vc +
                '}';
    }

    public MyVectorClockMessage(Host sender, VectorClock vc) {
        super(MSG_ID);
        this.sender = sender;
        this.vc = vc;
    }

    public Host getSender() {
        return sender;
    }

    public VectorClock getVectorClock() {
        return vc;
    }

    public static ISerializer<MyVectorClockMessage> serializer = new ISerializer<MyVectorClockMessage>() {
        @Override
        public void serialize(MyVectorClockMessage vectorClockMessage, ByteBuf out) throws IOException {
            Host.serializer.serialize(vectorClockMessage.sender, out);
            VectorClock.serializer.serialize(vectorClockMessage.vc, out);
        }

        @Override
        public MyVectorClockMessage deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
            VectorClock vc = VectorClock.serializer.deserialize(in);
            return new MyVectorClockMessage(sender, vc);
        }
    };
}
