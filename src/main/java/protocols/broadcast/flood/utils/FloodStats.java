package protocols.broadcast.flood.utils;

public class FloodStats {

    public static int sentFlood;
    public static int sentSendVC;
    public static int sentVC;
    public static int sentSyncOps;
    public static int sentSyncFlood;

    public static int receivedFlood;
    public static int receivedDupesFlood;
    public static int receivedSendVC;
    public static int receivedVC;
    public static int receivedSyncOps;
    public static int receivedSyncFlood;
    public static int receivedDupesSyncFlood;

    public FloodStats() {}

    public void incrementSentFlood() {
        sentFlood++;
    }

    public void incrementSentSendVC() {
        sentSendVC++;
    }

    public void incrementSentVC() {
        sentVC++;
    }

    public void incrementSentSyncOps() {
        sentSyncOps++;
    }

    public void incrementSentSyncFloodBy(int value) {
        sentSyncFlood += value;
    }

    public void incrementReceivedFlood() {
        receivedFlood++;
    }

    public void incrementReceivedDupesFlood() {
        receivedDupesFlood++;
    }

    public void incrementReceivedSendVC() {
        receivedSendVC++;
    }

    public void incrementReceivedVC() {
        receivedVC++;
    }

    public void incrementReceivedSyncOps() {
        receivedSyncOps++;
    }

    public void incrementReceivedSyncFlood() {
        receivedSyncFlood++;
    }

    public void incrementReceivedDupesSyncFlood() {
        receivedDupesSyncFlood++;
    }

}