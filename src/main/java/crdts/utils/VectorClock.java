package crdts.utils;

import crdts.operations.vc.OperationVC;
import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MySerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class VectorClock {

    private static final Logger logger = LogManager.getLogger(VectorClock.class);

    Map<Host, Integer> clock;

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
        if(value == null)
            this.clock.put(host, 1);
        else
            this.clock.put(host, ++value);
    }

    public int getHostClock(Host host) {
        return this.clock.getOrDefault(host,0);
    }

    public Set<Host> getHosts() {
        return this.clock.keySet();
    }

    public boolean canExecuteOperation(OperationVC op) {
        //Check clock of sender of operation
        Host sender = op.getSender();
        int senderClock = op.getSenderClock();
        if(senderClock > this.clock.getOrDefault(sender, 0) + 1) {
            logger.debug("Missing operation from sender");
            logger.debug("Sender clock: {}", senderClock);
            logger.debug("Local clock: {}", this.clock);
            return false;
        }

        //Check for missing dependencies from other hosts
        for (Map.Entry<Host, Integer> entry : op.getVectorClock().getClock().entrySet()) {
            Host h = entry.getKey();
            Integer msgClock = entry.getValue();
            if (!h.equals(sender) && this.clock.getOrDefault(h, 0) < msgClock) {
                logger.debug("Missing dependency from {}", h);
                logger.debug("Host clock: {}", msgClock);
                logger.debug("Local clock: {}", this.clock.getOrDefault(h, 0));
                return false;
            }
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
