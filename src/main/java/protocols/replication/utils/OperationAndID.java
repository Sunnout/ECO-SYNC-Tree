package protocols.replication.utils;

import crdts.operations.vc.OperationVC;

import java.util.UUID;

public class OperationAndID {

    private OperationVC op;
    private UUID id;
    private boolean isCreateOp;

    public OperationAndID(OperationVC op, UUID id, boolean isCreateOp) {
        this.op = op;
        this.id = id;
        this.isCreateOp = isCreateOp;
    }

    public OperationVC getOp() {
        return op;
    }

    public UUID getId() {
        return id;
    }

    public boolean isCreateOp() {
        return isCreateOp;
    }
}
