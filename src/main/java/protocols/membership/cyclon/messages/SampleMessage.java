package protocols.membership.cyclon.messages;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.membership.cyclon.components.Connection;

import java.io.IOException;
import java.util.*;

public class SampleMessage extends ProtoMessage {

    public final static short MSG_ID = 1001;

    private final List<Connection> sample;

    public SampleMessage(List<Connection> sample) {
        super(MSG_ID);
        this.sample = sample;
    }

    public List<Connection> getSample() {
        return sample;
    }

    @Override
    public String toString() {
        return "ShuffleMessage{" +
                "subset=" + sample +
                '}';
    }

    public static ISerializer<SampleMessage> serializer = new ISerializer<SampleMessage>() {
        @Override
        public void serialize(SampleMessage sampleMessage, ByteBuf out) throws IOException {
            out.writeInt(sampleMessage.sample.size());
            for (Connection con : sampleMessage.sample) {
                Host.serializer.serialize(con.getHost(), out);
                out.writeInt(con.getAge());
            }
        }

        @Override
        public SampleMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            List<Connection> subset = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                subset.add(new Connection(Host.serializer.deserialize(in), in.readInt()));
            return new SampleMessage(subset);
        }
    };
}
