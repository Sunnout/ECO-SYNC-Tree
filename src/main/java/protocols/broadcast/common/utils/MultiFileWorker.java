package protocols.broadcast.common.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.GossipMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.*;
import java.util.*;

public class MultiFileWorker {

    private static final Logger logger = LogManager.getLogger(MultiFileWorker.class);

    private final VectorClock firstVC;
    private final String fileName;
    private final File file;
    private final DataOutputStream dos;
    private final Map<Host, NavigableMap<Integer, Pair<Long, Integer>>> index;
    private final int indexSpacing;
    private int nOps;
    private long nBytes;

    public MultiFileWorker(VectorClock firstVC, String fileName, int indexSpacing) throws FileNotFoundException {
        this.firstVC = firstVC;
        this.fileName = fileName;
        this.file = new File(fileName);
        if(!this.file.getParentFile().mkdirs())
            logger.warn("Directory for files already existed or was not created.");
        this.dos = new DataOutputStream(new FileOutputStream(this.file));
        this.index = new HashMap<>();
        this.indexSpacing = indexSpacing;
    }

    public void writeOperationToFile(long time, GossipMessage msg) throws IOException {
        Host sender = msg.getOriginalSender();
        int senderClock = msg.getSenderClock();
        ByteBuf buf = Unpooled.buffer();
        buf.writeLong(time);
        GossipMessage.serializer.serialize(msg, buf);
        byte[] serGossipMsg = new byte[buf.readableBytes()];
        buf.readBytes(serGossipMsg);
        dos.write(serGossipMsg);
        dos.flush();

        if(senderClock % indexSpacing == 0) {
            index.computeIfAbsent(sender, k -> treeMapWithDefaultEntry()).put(senderClock, Pair.of(nBytes, nOps));
        }
        nOps++;
        nBytes += serGossipMsg.length;
    }

    public List<byte[]> readSyncOpsFromFile(VectorClock neighbourClock, VectorClock myClock) {
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

                for (int i = min.getRight(); i < nOps; i++) {
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
                logger.debug("READ FROM FILE in {} ms started {} of {}", endTime - startTime, min.getRight(), nOps);
            } catch (IOException e) {
                logger.error("Error reading missing ops from file", e);
            }
        } else {
            logger.debug("DID NOT OPEN FILE");
        }
        return gossipMessages;
    }

    public List<GossipMessage> getMyLateOperations(Host myself, VectorClock myVC, int mySeqNumber) {
        List<GossipMessage> gossipMessages = new LinkedList<>();
        int myLateSeqNumber = myVC.getHostClock(myself);
        if(myLateSeqNumber < mySeqNumber) {
            try (FileInputStream fis = new FileInputStream(this.file);
                 BufferedInputStream bis = new BufferedInputStream(fis);
                 DataInputStream dis = new DataInputStream(bis)) {

                for (int i = 0; i < nOps && myLateSeqNumber < mySeqNumber; i++) {
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
            }
        }
        return gossipMessages;
    }

    public void deleteFile() throws IOException {
        dos.close();
        if(!file.delete())
            logger.warn("File {} was not deleted.", fileName);
    }

    public VectorClock getFirstVC() {
        return this.firstVC;
    }

    private NavigableMap<Integer, Pair<Long, Integer>> treeMapWithDefaultEntry() {
        NavigableMap<Integer, Pair<Long, Integer>> map = new TreeMap<>();
        map.put(0, Pair.of(0L, 0));
        return map;
    }
}
