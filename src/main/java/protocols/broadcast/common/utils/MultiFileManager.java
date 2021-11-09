package protocols.broadcast.common.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.GossipMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class MultiFileManager {

    private static final Logger logger = LogManager.getLogger(MultiFileManager.class);

    private static final String DIRECTORY_PATH = "/data/";

    private final ArrayList<Pair<Long, MultiFileWorker>> fileWorkers;
    private final String fileNamePrefix;
    private final int indexSpacing;
    private final long garbageCollectionTimeout;
    private final long garbageCollectionTTL;

    public MultiFileManager(long garbageCollectionTimeout, long garbageCollectionTTL, int indexSpacing,  Host myself) {
        this.fileWorkers = new ArrayList<>();
        this.fileNamePrefix = "ops-" + myself +"-";
        this.indexSpacing = indexSpacing;
        this.garbageCollectionTimeout = garbageCollectionTimeout;
        this.garbageCollectionTTL = garbageCollectionTTL;
    }

    public void writeOperationToFile(GossipMessage msg, VectorClock vc) throws IOException {
        long time = calculateTime();
        if(fileWorkers.isEmpty() || fileWorkers.get(fileWorkers.size()-1).getLeft() < time) {
            logger.debug("Opening new file {}",fileNamePrefix + time);
            fileWorkers.add(Pair.of(time, new MultiFileWorker(vc, DIRECTORY_PATH + fileNamePrefix + time, indexSpacing)));
        }

        fileWorkers.get(fileWorkers.size()-1).getRight().writeOperationToFile(time, msg);
    }

    public List<byte[]> readSyncOpsFromFile(VectorClock neighbourClock, VectorClock myClock) {
        int startIndex = 0;
        for(int i = fileWorkers.size()-1; i >= 0; i--) {
            if(neighbourClock.greaterOrEqualThan(fileWorkers.get(i).getRight().getFirstVC())) {
                startIndex = i;
                break;
            }
        }

        if(fileWorkers.size() > 0)
            logger.debug("Started reading at {}; {}/{}", fileWorkers.get(startIndex).getLeft(), startIndex, fileWorkers.size()-1);
        else
            logger.debug("No files to read");

        List<byte[]> gossipMessages = new LinkedList<>();
        for(int j = startIndex; j < fileWorkers.size(); j++) {
            gossipMessages.addAll(fileWorkers.get(j).getRight().readSyncOpsFromFile(neighbourClock, myClock));
        }

        return gossipMessages;
    }

    public List<GossipMessage> getMyLateOperations(Host myself, VectorClock myVC, int mySeqNumber) {
        List<GossipMessage> gossipMessages = new LinkedList<>();
        for (Pair<Long, MultiFileWorker> fileWorker : fileWorkers) {
            gossipMessages.addAll(fileWorker.getRight().getMyLateOperations(myself, myVC, mySeqNumber));
        }
        return gossipMessages;
    }

    public void garbageCollectOperations() throws IOException {
        long time = calculateTime();
        Iterator<Pair<Long, MultiFileWorker>> it = fileWorkers.iterator();

        while(it.hasNext()) {
            Pair<Long, MultiFileWorker> mf = it.next();
            if(time - mf.getLeft() > (garbageCollectionTTL/garbageCollectionTimeout)) {
                logger.debug("Deleting file {}", mf.getLeft());
                mf.getRight().deleteFile();
                it.remove();
            } else {
                break;
            }
        }
    }
    
    public long getCurrentDiskUsage() {
        long diskUsage = 0;
        for(Pair<Long, MultiFileWorker> pair : fileWorkers) {
            diskUsage += pair.getValue().getNBytes();
        }
        return diskUsage;
    }

    private long calculateTime() {
        return System.currentTimeMillis()/garbageCollectionTimeout;
    }

}
