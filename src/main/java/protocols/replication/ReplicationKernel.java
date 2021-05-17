package protocols.replication;

import crdts.operations.*;
import crdts.utils.VectorClock;
import datatypes.*;
import exceptions.NoSuchCrdtType;
import exceptions.NoSuchDataType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.plumtree.notifications.OriginalVectorClockNotification;
import protocols.broadcast.plumtree.notifications.PendingSyncNotification;
import protocols.broadcast.plumtree.notifications.ReplyVectorClockNotification;
import protocols.broadcast.plumtree.notifications.SyncOpsNotification;
import protocols.broadcast.plumtree.requests.MyVectorClockRequest;
import protocols.broadcast.plumtree.requests.MyVectorClockReply;
import protocols.broadcast.plumtree.requests.SyncOpsRequest;
import protocols.replication.notifications.*;
import protocols.replication.requests.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationKernel extends GenericProtocol implements CRDTCommunicationInterface {

    private static final Logger logger = LogManager.getLogger(ReplicationKernel.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "ReplicationKernel";
    public static final short PROTOCOL_ID = 600;

    private static final String CREATE_CRDT = "create";

    //CRDT Types
    private static final String COUNTER = "counter";
    private static final String LWW_REGISTER = "lww_register";
    private static final String OR_SET = "or_set";
    private static final String OR_MAP = "or_map";

    //Data Types
    private static final String INTEGER = "int";
    private static final String SHORT = "short";
    private static final String LONG = "long";
    private static final String FLOAT = "float";
    private static final String DOUBLE = "double";
    private static final String STRING = "string";
    private static final String BOOLEAN = "boolean";
    private static final String BYTE = "byte";

    private short broadcastId; //Broadcast protocol ID
    private final Host myself;

    //Replication kernel variables
    private VectorClock vectorClock; //Local vector clock
    private Map<String, KernelCRDT> crdtsById; //Map that stores CRDTs by their ID
    private Map<String, Set<Host>> hostsByCrdt; //Map that stores the hosts that replicate a given CRDT
    private Queue<byte[]> bufferedOps; //Queue of operations received while not synched
    private boolean synched;

    public static List<Operation> causallyOrderedOps; //List of causally ordered received operations
    private int seqNumber;

    //Debug variables
    public static int sentOps;
    public static int receivedOps;
    public static int executedOps;

    //Serializers
    public static Map<String, MyOpSerializer> opSerializers = initializeOperationSerializers(); //Static map of operation serializers for each crdt type
    public Map<String, List<MySerializer>> dataSerializers; //Map of data type serializers by crdt ID


    public ReplicationKernel(Properties properties, Host myself, short broadcastId) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.broadcastId = broadcastId;
        this.myself = myself;

        initializeVectorClock();
        this.crdtsById = new ConcurrentHashMap<>();
        this.hostsByCrdt = new ConcurrentHashMap<>();
        causallyOrderedOps = new ArrayList<>();
        this.bufferedOps = new LinkedList<>();
        this.synched = false;

        this.dataSerializers = new HashMap<>();

        /* --------------------- Register Request Handlers --------------------- */
        registerRequestHandler(GetCRDTRequest.REQUEST_ID, this::uponGetCRDTRequest);
        registerRequestHandler(ReleaseCRDTRequest.REQUEST_ID, this::uponReleaseCRDTRequest);
        registerRequestHandler(DownstreamRequest.REQUEST_ID, this::uponDownstreamRequest);

        /* --------------------- Register Notification Handlers --------------------- */
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliverNotification);
        subscribeNotification(PendingSyncNotification.NOTIFICATION_ID, this::uponPendingSyncNotification);
        subscribeNotification(OriginalVectorClockNotification.NOTIFICATION_ID, this::uponOriginalVectorClockNotification);
//        subscribeNotification(ReplyVectorClockNotification.NOTIFICATION_ID, this::uponReplyVectorClockNotification);
        subscribeNotification(SyncOpsNotification.NOTIFICATION_ID, this::uponSyncOpsNotification);

    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for event from the application
    }

    /* --------------------------------- Requests --------------------------------- */

    private void uponGetCRDTRequest(GetCRDTRequest request, short sourceProto) {
        try {
            String crdtId = request.getCrdtId();
            String crdtType = request.getCrdtType();
            String[] dataTypes = request.getDataType();
            Host sender = request.getSender();
            UUID msgId = request.getMsgId();

            logger.debug("Received get CRDT with id {} and type {} request: {}", crdtId, crdtType, msgId);

            KernelCRDT crdt = crdtsById.get(crdtId);
            if(crdt != null) {
                if(validateCrdtType(crdt, crdtType)) {
                    logger.debug("Sending CRDT with id {} to app", crdtId);
                    addHostToReplicationSet(crdtId, sender);
                    triggerNotification(new ReturnCRDTNotification(msgId, sender, crdt));
                } else {
                    logger.debug("CRDT with type different from {} already exists", crdtType);
                    triggerNotification(new CRDTAlreadyExistsNotification(msgId, sender, crdtId));
                }
            } else {
                handleCRDTCreation(crdtId, crdtType, dataTypes, sender, msgId);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponReleaseCRDTRequest(ReleaseCRDTRequest request, short sourceProto) {
        String crdtId = request.getCrdtId();
        Host sender = request.getSender();
        logger.debug("Received release CRDT with id {} request from {}: {}", crdtId, sender, request.getMsgId());
        logger.debug("Before: {}", hostsByCrdt.get(crdtId));
        removeHostFromReplicationSet(crdtId, sender);
        logger.debug("After: {}", hostsByCrdt.get(crdtId));
    }

    /**
     * Propagates local operations to other replication kernels after incrementing
     * and setting the operation's vector clock.
     * @param request
     * @param sourceProto
     */
    private void uponDownstreamRequest(DownstreamRequest request, short sourceProto) {
        UUID msgId = request.getMsgId();
        logger.info("Received downstream request: {}", msgId);

        Operation op = request.getOperation();
        incrementAndSetVectorClock(op);
        causallyOrderedOps.add(op);
        try {
            broadcastOperation(false, msgId, request.getSender(), op);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /* --------------------------------- Notifications --------------------------------- */

    /**
     * Processes operations received from other replication kernels.
     * @param notification
     * @param sourceProto
     */
    private void uponDeliverNotification(DeliverNotification notification, short sourceProto) {
        Host sender = notification.getSender();
        try {
            if (!sender.equals(myself)) {
                receivedOps++;
                if(!synched) {
                    logger.info("Buffering operation");
                    this.bufferedOps.add(notification.getMsg());
                } else {
                    Operation op = deserializeOperation(notification.getMsg());
                    logger.debug("Executing operation");
                    executeOperation(op.getSender(), op);
                }
            } else {
                executedOps++;
                sentOps++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponPendingSyncNotification(PendingSyncNotification notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        logger.info("Received {} with {}", notification, neighbour);
        sendRequest(new MyVectorClockRequest(UUID.randomUUID(), myself, neighbour, this.vectorClock), broadcastId);
    }

    private void uponOriginalVectorClockNotification(OriginalVectorClockNotification notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        logger.info("Received {} with {}", notification, neighbour);
//        sendRequest(new MyVectorClockReply(UUID.randomUUID(), myself, neighbour, this.vectorClock), broadcastId);
        List<byte[]> ops = null;
        try {
            ops = getMissingSerializedOperations(notification.getVectorClock());
        } catch (IOException e) {
            e.printStackTrace();
        }
        sendRequest(new SyncOpsRequest(UUID.randomUUID(), myself, notification.getNeighbour(), null, ops), broadcastId);
    }

//    private void uponReplyVectorClockNotification(ReplyVectorClockNotification notification, short sourceProto) {
//        Host neighbour = notification.getNeighbour();
//        logger.debug("Received {} with {}", notification, neighbour);
//        List<byte[]> ops = null;
//        try {
//            ops = getMissingSerializedOperations(notification.getVectorClock());
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        sendRequest(new SyncOpsRequest(UUID.randomUUID(), myself, notification.getNeighbour(), ops), broadcastId);
//    }

    private void uponSyncOpsNotification(SyncOpsNotification notification, short sourceProto) {
        logger.info("Received {} from {}", notification, notification.getNeighbour());

        try {
            //Reply to synchronization op with your ops
            if(notification.getVectorClock() != null) {
                List<byte[]> ops = getMissingSerializedOperations(notification.getVectorClock());
                sendRequest(new SyncOpsRequest(UUID.randomUUID(), myself, notification.getNeighbour(), null, ops), broadcastId);
            }

            //Execute operations from synchronization
            for (byte[] serOp : notification.getOperations()) {
                Operation op = deserializeOperation(serOp);
                logger.info("Sync operation with {}: {}", notification.getNeighbour(), op);
                executeOperation(notification.getNeighbour(), op);
                receivedOps++;
            }

            //Execute buffered operations
            for(byte[] serOp : bufferedOps) {
                Operation op = deserializeOperation(serOp);
                Host h = op.getSender();
                int clock = op.getSenderClock();
                if(this.vectorClock.getHostClock(h) < clock) {
                    logger.info("Buffered operation: {}", op);
                    executeOperation(h, op);
                }
                //TODO: test
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.synched = true;
    }


    /* --------------------------------- Interface Methods --------------------------------- */

    public void downstream(DownstreamRequest request, short sourceProto) {
        Operation op = request.getOperation();
        sendRequest(new DownstreamRequest(request.getMsgId(), request.getSender(), op), PROTOCOL_ID);
    }


    /* --------------------------------- Auxiliary Methods --------------------------------- */

    private List<byte[]> getMissingSerializedOperations(VectorClock vc) throws IOException {
        int index = 0;
        for (Operation op : causallyOrderedOps) {
            Host h = op.getSender();
            int clock = op.getSenderClock();
            if (vc.getHostClock(h) < clock)
                break;
            index++;
        }
        List<byte[]> ops = new LinkedList<>();

        for(int i = index; i < causallyOrderedOps.size(); i++) {
            Operation op = causallyOrderedOps.get(i);
            byte[] serializedOp = serializeOperation(op instanceof CreateOperation, op);
            ops.add(serializedOp);
        }
        return ops;
    }

    private Operation deserializeOperation(byte[] msg) throws IOException {
        ByteBuf buf = Unpooled.buffer().writeBytes(msg);
        String crdtId = Operation.crdtIdFromByteArray(buf);
        String crdtType = Operation.crdtTypeFromByteArray(buf);
        String opType = Operation.opTypeFromByteArray(buf);
        Operation op;

        if (opType.equals(CREATE_CRDT)) {
            op = CreateOperation.serializer.deserialize(null, buf);
        } else {
            MySerializer[] serializers = dataSerializers.get(crdtId).toArray(new MySerializer[2]);
            op = (Operation) opSerializers.get(crdtType).deserialize(serializers, buf);
        }
        return op;
    }

    private byte[] serializeOperation(boolean isCreateOp, Operation op) throws IOException {
        ByteBuf buf = Unpooled.buffer();
        if(isCreateOp) {
            CreateOperation.serializer.serialize((CreateOperation)op, null, buf);
        } else {
            MySerializer[] serializers = dataSerializers.get(op.getCrdtId()).toArray(new MySerializer[2]);
            opSerializers.get(op.getCrdtType()).serialize(op, serializers, buf);
        }
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);
        return payload;
    }

    private void broadcastOperation(boolean isCreateOp, UUID msgId, Host sender, Operation op) throws IOException {
        byte[] payload = serializeOperation(isCreateOp, op);
        sendRequest(new BroadcastRequest(msgId, sender, payload), broadcastId);
    }

    private void executeOperation(Host sender, Operation op) throws IOException {
        causallyOrderedOps.add(op);

        String crdtId = op.getCrdtId();
        String crdtType = op.getCrdtType();

        if(op instanceof CreateOperation) {
            if (crdtsById.get(crdtId) == null) {
                createNewCrdt(crdtId, crdtType, ((CreateOperation)op).getDataTypes(), sender);
            } else {
                addHostToReplicationSet(crdtId, sender);
            }
        } else {
            crdtsById.get(crdtId).upstream(op);
        }
        this.vectorClock.incrementClock(sender);
        executedOps++;
    }

    private void handleCRDTCreation(String crdtId, String crdtType, String[] dataTypes, Host sender, UUID msgId) throws IOException {
        logger.debug("Creating new CRDT with id {} and type {}", crdtId, crdtType);
        KernelCRDT crdt = createNewCrdt(crdtId, crdtType, dataTypes, sender);
        triggerNotification(new ReturnCRDTNotification(msgId, sender, crdt));
        CreateOperation op = new CreateOperation(myself, ++seqNumber, CREATE_CRDT, crdtId, crdtType, dataTypes);
        causallyOrderedOps.add(op);
        broadcastOperation(true, msgId, sender, op);
    }

    /**
     * Creates a map with the operation serializers for each crdt type.
     * @return the created map.
     */
    private static Map<String, MyOpSerializer> initializeOperationSerializers() {
        Map<String, MyOpSerializer> map = new HashMap<>();
        map.put(COUNTER, CounterOperation.serializer);
        map.put(LWW_REGISTER, RegisterOperation.serializer);
        map.put(OR_SET, SetOperation.serializer);
        map.put(OR_MAP, MapOperation.serializer);
        return map;
    }

    /**
     * Maps each CRDT to its own dataType serializer. If the CRDT is a map
     * two serializers must be added (the first for the key, the second for
     * the value).
     * @param crdtId - ID of the CRDT.
     * @param crdtType - type of the CRDT.
     * @param dataTypes - data types of the CRDT.
     */
    private void registerDataSerializer(String crdtId, String crdtType, String[] dataTypes) {
        List<MySerializer> serializerList = new ArrayList<>(2);
        switch (dataTypes[0]) {
            case INTEGER:
                serializerList.add(0, IntegerType.serializer);
                break;
            case SHORT:
                serializerList.add(0, ShortType.serializer);
                break;
            case LONG:
                serializerList.add(0, LongType.serializer);
                break;
            case FLOAT:
                serializerList.add(0, FloatType.serializer);
                break;
            case DOUBLE:
                serializerList.add(0, DoubleType.serializer);
                break;
            case STRING:
                serializerList.add(0, StringType.serializer);
                break;
            case BOOLEAN:
                serializerList.add(0, BooleanType.serializer);
                break;
            case BYTE:
                serializerList.add(0, ByteType.serializer);
                break;
            default:
                throw new NoSuchDataType(dataTypes[0]);
        }
        dataSerializers.put(crdtId, serializerList);
        if(crdtType.equals(OR_MAP))
            addExtraDataSerializerForMap(crdtId, dataTypes);
    }

    /**
     * Maps the second serializer (value serializer) for map CRDTs.
     * @param crdtId - ID of the CRDT.
     * @param dataTypes - data types of the CRDT.
     */
    private void addExtraDataSerializerForMap(String crdtId, String[] dataTypes) {
        switch (dataTypes[1]) {
            case INTEGER:
                dataSerializers.get(crdtId).add(1, IntegerType.serializer);
                break;
            case SHORT:
                dataSerializers.get(crdtId).add(1, ShortType.serializer);
                break;
            case LONG:
                dataSerializers.get(crdtId).add(1, LongType.serializer);
                break;
            case FLOAT:
                dataSerializers.get(crdtId).add(1, FloatType.serializer);
                break;
            case DOUBLE:
                dataSerializers.get(crdtId).add(1, DoubleType.serializer);
                break;
            case STRING:
                dataSerializers.get(crdtId).add(1, StringType.serializer);
                break;
            case BOOLEAN:
                dataSerializers.get(crdtId).add(1, BooleanType.serializer);
                break;
            case BYTE:
                dataSerializers.get(crdtId).add(1, ByteType.serializer);
                break;
            default:
                throw new NoSuchDataType(dataTypes[1]);
        }
    }

    /**
     * Validates that the CRDT is of the given crdtType.
     * @param crdt - CRDT to validate.
     * @param crdtType - type to check against.
     * @return true if the crdt is of crdtType, false otherwise.
     */
    private boolean validateCrdtType(KernelCRDT crdt, String crdtType) {
        switch(crdtType) {
            case COUNTER:
                return crdt instanceof OpCounterCRDT;
            case LWW_REGISTER:
                return crdt instanceof LWWRegisterCRDT;
            case OR_SET:
                return crdt instanceof ORSetCRDT;
            case OR_MAP:
                return crdt instanceof ORMapCRDT;
            default:
                throw new NoSuchCrdtType(crdtType);
        }
    }

    /**
     * Creates a new CRDT of the given dataType with the given ID
     * and registers it in the kernel. Adds the host to the replication
     * set of this replica and registers its serializers.
     * @param crdtId - ID of the CRDT.
     * @param crdtType - type of the CRDT.
     * @param dataTypes - data types of the CRDT.
     * @param sender - host that created the CRDT.
     * @return the new crdt.
     */
    private KernelCRDT createNewCrdt(String crdtId, String crdtType, String[] dataTypes, Host sender) {
        KernelCRDT crdt;
        switch(crdtType) {
            case COUNTER:
                crdt = new OpCounterCRDT(this, crdtId);
                break;
            case LWW_REGISTER:
                crdt = new LWWRegisterCRDT(this, crdtId);
                break;
            case OR_SET:
                crdt = new ORSetCRDT(this, crdtId);
                break;
            case OR_MAP:
                crdt = new ORMapCRDT(this, crdtId);
                break;
            default:
                throw new NoSuchCrdtType(crdtType);
        }
        crdtsById.put(crdtId, crdt);
        addHostToReplicationSet(crdtId, sender);
        registerDataSerializer(crdtId, crdtType, dataTypes);
        return crdt;
    }

    /**
     * Adds the host to the replication set of the crdt with the given ID.
     * @param crdtId - ID of the CRDT.
     * @param host - host to add to the replication set.
     */
    private void addHostToReplicationSet(String crdtId, Host host) {
        Set<Host> replicas = hostsByCrdt.get(crdtId);
        if(replicas != null)
            replicas.add(host);
        else {
            replicas = new HashSet<>();
            replicas.add(host);
            hostsByCrdt.put(crdtId, replicas);
        }
    }

    /**
     * Removes the host from the replication set of the crdt with the given ID.
     * @param crdtId - ID of the CRDT.
     * @param host - host to remove from the replication set.
     */
    private void removeHostFromReplicationSet(String crdtId, Host host) {
        Set<Host> replicas = hostsByCrdt.get(crdtId);
        replicas.remove(host);
    }

    /**
     * Initializes local vector clock with zero in the host's slot.
     */
    private void initializeVectorClock() {
        this.vectorClock = new VectorClock(myself);
    }

    private void incrementAndSetVectorClock(Operation op) {
        this.vectorClock.incrementClock(myself);
        op.setSender(myself);
        op.setSenderClock(++seqNumber);
        logger.debug("Set local seqNumber to {}", seqNumber);
    }

}
