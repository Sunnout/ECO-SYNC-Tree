package protocols.broadcast.periodicpull.utils;

public class PeriodicPullStats {

    public static int sentVC;
    public static int sentSyncOps;
    public static int sentSyncPull;

    public static int receivedVC;
    public static int receivedSyncOps;
    public static int receivedSyncPull;
    public static int receivedDupes;

    public PeriodicPullStats() {}

    public void incrementSentSyncPullBy(int value) {
        sentSyncPull += value;
    }

    public void incrementSentVC() {
        sentVC++;
    }

    public void incrementSentSyncOps() {
        sentSyncOps++;
    }

    public void incrementReceivedDupes() {
        receivedDupes++;
    }

    public void incrementReceivedVC() {
        receivedVC++;
    }

    public void incrementReceivedSyncOps() {
        receivedSyncOps++;
    }

    public void incrementReceivedSyncPull() {
        receivedSyncPull++;
    }
}