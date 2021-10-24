package protocols.broadcast.common.utils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.SynchronizationMessage;
import protocols.broadcast.plumtree.messages.GossipMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class MultiFileManager {

    private static final Logger logger = LogManager.getLogger(MultiFileManager.class);

    private static final String DIRECTORY_PATH = "/tmp/data/";

    private final ArrayList<Pair<Long, MultiFileWorker>> fileWorkers;
    private final String fileNamePrefix;
    private final int indexSpacing;

    public MultiFileManager(Properties properties, Host myself) {
        this.fileWorkers = new ArrayList<>();
        this.fileNamePrefix = "ops-" + myself +"-";
        this.indexSpacing = Integer.parseInt(properties.getProperty("index_spacing", "100"));
    }

    public void writeOperationToFile(GossipMessage msg, VectorClock vc) throws IOException {
        long time = calculateTime();
        if(fileWorkers.isEmpty() || fileWorkers.get(fileWorkers.size()-1).getLeft() < time) {
            logger.debug("Opening new file {}",fileNamePrefix + time);
            fileWorkers.add(Pair.of(time, new MultiFileWorker(vc, DIRECTORY_PATH + fileNamePrefix + time, indexSpacing)));
        }

        fileWorkers.get(fileWorkers.size()-1).getRight().writeOperationToFile(time, msg);
    }

    public SynchronizationMessage readSyncOpsFromFile(UUID mid, VectorClock neighbourClock, VectorClock myClock, StateAndVC stateAndVC) {
        int startIndex = 0;
        for(int i = fileWorkers.size()-1; i >= 0; i--) {
            if(!neighbourClock.greaterOrEqualThan(fileWorkers.get(i).getRight().getFirstVC())) {
                startIndex = i;
                break;
            }
        }

        logger.debug("Started reading at {}; {}/{}", fileWorkers.get(startIndex).getLeft(), startIndex, fileWorkers.size()-1);
        List<byte[]> gossipMessages = new LinkedList<>();
        for(int j = startIndex; j < fileWorkers.size(); j++) {
            gossipMessages.addAll(fileWorkers.get(j).getRight().readSyncOpsFromFile(neighbourClock, myClock));
        }

        return new SynchronizationMessage(mid, stateAndVC, gossipMessages);
    }

    public List<GossipMessage> getMyLateOperations(Host myself, VectorClock myVC, int mySeqNumber) {
        List<GossipMessage> gossipMessages = new LinkedList<>();
        for (Pair<Long, MultiFileWorker> fileWorker : fileWorkers) {
            gossipMessages.addAll(fileWorker.getRight().getMyLateOperations(myself, myVC, mySeqNumber));
        }
        return gossipMessages;
    }

    public void garbageCollectOperations(int ttlMin) throws IOException {
        long time = calculateTime();
        Iterator<Pair<Long, MultiFileWorker>> it = fileWorkers.iterator();

        while(it.hasNext() && (time - it.next().getLeft()) > ttlMin) {
            Pair<Long, MultiFileWorker> mf = it.next();
            if(time - mf.getLeft() > ttlMin) {
                mf.getRight().deleteFile();
                it.remove();
            } else {
                break;
            }
        }
    }

    private long calculateTime() {
        return System.currentTimeMillis()/6000;
    }

}
