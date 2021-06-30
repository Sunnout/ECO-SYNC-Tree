package protocols.replication.utils;

import java.util.Comparator;

public class SortOpsByHostClock implements Comparator<OperationAndID> {

    @Override
    public int compare(OperationAndID o1, OperationAndID o2) {
        int v1 = o1.getOp().getSenderClock();
        int v2 = o2.getOp().getSenderClock();
        if (v1 < v2)
            return -1;
        else if (v1 > v2)
            return 1;
        else
            return 0;
    }

}
