package protocols.broadcast.common.utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.replication.crdts.serializers.MySerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class VectorClock {

    private Map<Host, Integer> clock;

    public VectorClock(Host sender) {
        this.clock = new HashMap<>();
        this.clock.put(sender, 0);
    }

    public VectorClock(Map<Host, Integer> clock) {
        this.clock = new HashMap<>(clock);
    }

    public Map<Host, Integer> getClock() {
        return this.clock;
    }

    public void incrementClock(Host host) {
        Integer value = this.clock.get(host);
        if (value == null)
            this.clock.put(host, 1);
        else
            this.clock.put(host, ++value);
    }

    public int getHostClock(Host host) {
        return this.clock.getOrDefault(host,0);
    }

    public void setHostClock(Host host, int value) {
        this.clock.put(host, value);
    }

    public Set<Host> getHosts() {
        return this.clock.keySet();
    }

    public boolean isEmptyExceptFor(Host host) {
        for (Map.Entry<Host, Integer> e : this.clock.entrySet()) {
            Host clockHost = e.getKey();
            Integer clockValue = e.getValue();
            if(!clockHost.equals(host) && (clockValue != null || clockValue != 0))
                return false;
        }
        return true;
    }

    public boolean greaterOrEqualThan(VectorClock otherVC) {
        for (Map.Entry<Host, Integer> entry: otherVC.getClock().entrySet()) {
            Host host = entry.getKey();
            int value = entry.getValue();
            if(this.getHostClock(host) < value)
                return false;
        }
        return true;
    }



    public static MySerializer<VectorClock> serializer = new MySerializer<VectorClock>() {
        @Override
        public void serialize(VectorClock vc, ByteBuf out) throws IOException {
            Map<Host, Integer> clocks = vc.getClock();
            out.writeInt(clocks.entrySet().size());
            for (Map.Entry<Host, Integer> entry : clocks.entrySet()) {
                Host k = entry.getKey();
                int v = entry.getValue();
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
