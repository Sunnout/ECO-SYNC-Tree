package crdts.utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MySerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VectorClock {

    Map<Host, Integer> clock;

    public VectorClock(Host sender) {
        this.clock = new HashMap<>();
        this.clock.put(sender, 0);
    }

    public VectorClock(Map<Host, Integer> clock) {
        this.clock = clock;
    }

    public Map<Host, Integer> getClock() {
        return this.clock;
    }

    public void incrementClock(Host host) {
        Integer value = this.clock.get(host);
        if(value == null)
            this.clock.put(host, 1);
        else
            this.clock.put(host, ++value);
    }

    public int getHostClock(Host host) {
        return this.clock.getOrDefault(host,0);
    }

    public static MySerializer<VectorClock> serializer = new MySerializer<VectorClock>() {
        @Override
        public void serialize(VectorClock vc, ByteBuf out) throws IOException {
            Map<Host, Integer> clocks = vc.getClock();
            out.writeInt(clocks.entrySet().size());
            for (Map.Entry<Host, Integer> entry : clocks.entrySet()) {
                Host k = entry.getKey();
                Integer v = entry.getValue();
                Host.serializer.serialize(k, out);
                out.writeInt(v);
            }
        }

        @Override
        public VectorClock deserialize(ByteBuf in) throws IOException {
            Map<Host, Integer> clocks = new HashMap<>();
            int size = in.readInt();
            for(int i = 0; i < size; i++)
                clocks.put(Host.serializer.deserialize(in), in.readInt());
            return new VectorClock(clocks);
        }
    };

    @Override
    public String toString() {
        return "VectorClock{" +
                "clock=" + clock +
                '}';
    }

}
