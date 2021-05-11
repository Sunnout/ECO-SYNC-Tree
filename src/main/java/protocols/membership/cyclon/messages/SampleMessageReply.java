package protocols.membership.cyclon.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.membership.cyclon.components.Connection;

public class SampleMessageReply extends ProtoMessage {

    public final static short MSG_ID = 1002;

    private final List<Connection> sample;

    public SampleMessageReply(List<Connection> sample) {
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

    public static ISerializer<SampleMessageReply> serializer = new ISerializer<SampleMessageReply>() {
        @Override
        public void serialize(SampleMessageReply sampleMessageReply, ByteBuf out) throws IOException {
            out.writeInt(sampleMessageReply.sample.size());
            for (Connection con : sampleMessageReply.sample) {
                Host.serializer.serialize(con.getHost(), out);
                out.writeInt(con.getAge());
            }
        }

        @Override
        public SampleMessageReply deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            List<Connection> subset = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                subset.add(new Connection(Host.serializer.deserialize(in), in.readInt()));
            return new SampleMessageReply(subset);
        }
    };
}
