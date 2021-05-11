package protocols.replication;

import protocols.replication.requests.DownstreamRequest;

public interface CRDTCommunicationInterface {

    /**
     * Called by each local CRDT to trigger the propagation of an operation
     * between replication kernels.
     * @param request - request that contains the operation to propagate.
     * @param sourceProto
     */
    void downstream(DownstreamRequest request, short sourceProto);

}
