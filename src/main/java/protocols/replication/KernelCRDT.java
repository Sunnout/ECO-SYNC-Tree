package protocols.replication;

import crdts.interfaces.GenericCRDT;
import crdts.operations.Operation;

interface KernelCRDT extends GenericCRDT {

    void upstream(Operation op);
    
}
