package protocols.broadcast.common.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.SynchronizationMessage;
import protocols.broadcast.plumtree.messages.GossipMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.*;
import java.util.*;

public class MyFileManager {

    private static final Logger logger = LogManager.getLogger(MyFileManager.class);

    private final File file;
    private final DataOutputStream dos;
    private final Map<Host, NavigableMap<Integer, Pair<Long, Integer>>> index;
    private int nExecuted;
    private long nBytes;
    private final int indexSpacing;

    public MyFileManager(Properties properties, Host myself) throws FileNotFoundException {
        this.file = new File("/tmp/data/ops-" + myself);
        if(!this.file.getParentFile().mkdirs())
            logger.warn("Directory for files already existed or was not created.");
        this.dos = new DataOutputStream(new FileOutputStream(this.file));
        this.index = new HashMap<>();
        this.indexSpacing = Integer.parseInt(properties.getProperty("index_spacing", "100"));

    }

    public void writeOperationToFile(GossipMessage msg) throws IOException {
        Host sender = msg.getOriginalSender();
        int senderClock = msg.getSenderClock();
        ByteBuf buf = Unpooled.buffer();
        buf.writeLong(System.currentTimeMillis());
        GossipMessage.serializer.serialize(msg, buf);
        byte[] serGossipMsg = new byte[buf.readableBytes()];
        buf.readBytes(serGossipMsg);
        dos.write(serGossipMsg);
        dos.flush();

        if(senderClock % indexSpacing == 0) {
            index.computeIfAbsent(sender, k -> treeMapWithDefaultEntry()).put(senderClock, Pair.of(nBytes, nExecuted));
        }
        nExecuted++;
        nBytes += serGossipMsg.length;
    }

    public SynchronizationMessage readSyncOpsFromFile(UUID mid, VectorClock neighbourClock, VectorClock myClock, StateAndVC stateAndVC) {
        long startTime = System.currentTimeMillis();
        Pair<Long, Integer> min = null;

        for (Host h : myClock.getHosts()) {
            int clock = neighbourClock.getHostClock(h);
            if(clock >= myClock.getHostClock(h)) continue;
            Map.Entry<Integer, Pair<Long, Integer>> indexEntry = index.computeIfAbsent(h, k -> treeMapWithDefaultEntry()).floorEntry(clock);
            if(min == null || indexEntry.getValue().getLeft() < min.getLeft())
                min = indexEntry.getValue();
        }

        List<byte[]> gossipMessages = new LinkedList<>();

        if(min != null) {
            try (FileInputStream fis = new FileInputStream(this.file);
                 BufferedInputStream bis = new BufferedInputStream(fis);
                 DataInputStream dis = new DataInputStream(bis)) {

                long skipped = fis.skip(min.getLeft());
                if (skipped != min.getLeft()) {
                    logger.error("SKIPPED {}, WANTED {} OF {}", skipped, min.getLeft(), nBytes);
                }

                for (int i = min.getRight(); i < nExecuted; i++) {
                    dis.readLong();
                    GossipMessage msg = GossipMessage.deserialize(dis);
                    Host h = msg.getOriginalSender();
                    int opClock = msg.getSenderClock();
                    if (neighbourClock.getHostClock(h) < opClock) {
                        ByteBuf buf = Unpooled.buffer();
                        GossipMessage.serializer.serialize(msg, buf);
                        byte[] serMsg = new byte[buf.readableBytes()];
                        buf.readBytes(serMsg);
                        gossipMessages.add(serMsg);
                    }
                }
                long endTime = System.currentTimeMillis();
                logger.debug("READ FROM FILE in {} ms started {} of {}", endTime - startTime, min.getRight(), nExecuted);
            } catch (IOException e) {
                logger.error("Error reading missing ops from file", e);
                e.printStackTrace();
            }
        } else {
            logger.debug("DID NOT OPEN FILE");
        }
        return new SynchronizationMessage(mid, stateAndVC, gossipMessages);
    }

    public List<GossipMessage> getMyLateOperations(Host myself, VectorClock myVC, int mySeqNumber) {
        List<GossipMessage> gossipMessages = new LinkedList<>();
        int myLateSeqNumber = myVC.getHostClock(myself);
        if(myLateSeqNumber < mySeqNumber) {
            try (FileInputStream fis = new FileInputStream(this.file);
                 BufferedInputStream bis = new BufferedInputStream(fis);
                 DataInputStream dis = new DataInputStream(bis)) {

                for (int i = 0; i < nExecuted && myLateSeqNumber < mySeqNumber; i++) {
                    dis.readLong();
                    GossipMessage msg = GossipMessage.deserialize(dis);
                    Host h = msg.getOriginalSender();
                    int opClock = msg.getSenderClock();
                    if (h.equals(myself) && opClock > myLateSeqNumber) {
                        gossipMessages.add(msg);
                        myLateSeqNumber++;
                    }
                }
            } catch (IOException e) {
                logger.error("Error reading my late ops from file", e);
                e.printStackTrace();
            }
        }
        return gossipMessages;
    }

    private NavigableMap<Integer, Pair<Long, Integer>> treeMapWithDefaultEntry() {
        NavigableMap<Integer, Pair<Long, Integer>> map = new TreeMap<>();
        map.put(0, Pair.of(0L, 0));
        return map;
    }
}
