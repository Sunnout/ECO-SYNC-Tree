package protocols.broadcast.plumtree.utils;

public class PlumtreeStats {

    public static int sentOps;
    public static int receivedOps;
    public static long executedOps;

    public static int sentTree;
    public static int sentGossip;
    public static int sentIHave;
    public static int sentGraft;
    public static int sentPrune;
    public static int sentReversePrune;
    public static int sentSendVC;
    public static int sentVC;
    public static int sentSyncOps;
    public static int sentSyncGossip;

    public static int receivedTree;
    public static int receivedGossip;
    public static int receivedDupesTree;
    public static int receivedDupesGossip;
    public static int receivedIHave;
    public static int receivedGraft;
    public static int receivedPrune;
    public static int receivedReversePrune;
    public static int receivedSendVC;
    public static int receivedVC;
    public static int receivedSyncOps;
    public static int receivedSyncGossip;
    public static int receivedDupesSyncGossip;

    public PlumtreeStats() {

    }

    public void incrementSentOps() {
        sentOps++;
    }

    public void incrementReceivedOps() {
        receivedOps++;
    }

    public void incrementExecutedOps() {
        executedOps++;
    }

    public void incrementSentTree() {
        sentTree++;
    }

    public void incrementSentGossip() {
        sentGossip++;
    }

    public void incrementSentIHave() {
        sentIHave++;
    }

    public void incrementSentGraft() {
        sentGraft++;
    }

    public void incrementSentPrune() {
        sentPrune++;
    }

    public void incrementSentReversePrune() {
        sentReversePrune++;
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

    public void incrementSentSyncGossipBy(int value) {
        sentSyncGossip += value;
    }

    public void incrementReceivedTree() {
        receivedTree++;
    }

    public void incrementReceivedGossip() {
        receivedGossip++;
    }

    public void incrementReceivedDupesTree() {
        receivedDupesTree++;
    }

    public void incrementReceivedDupesGossip() {
        receivedDupesGossip++;
    }

    public void incrementReceivedIHave() {
        receivedIHave++;
    }

    public void incrementReceivedGraft() {
        receivedGraft++;
    }

    public void incrementReceivedPrune() {
        receivedPrune++;
    }

    public void incrementReceivedReversePrune() {
        receivedReversePrune++;
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

    public void incrementReceivedSyncGossip() {
        receivedSyncGossip++;
    }

    public void incrementReceivedDupesSyncGossip() {
        receivedDupesSyncGossip++;
    }

}