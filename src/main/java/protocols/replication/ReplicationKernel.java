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
import protocols.broadcast.plumtree.notifications.SendVectorClockNotification;
import protocols.broadcast.plumtree.notifications.VectorClockNotification;
import protocols.broadcast.plumtree.notifications.NewPendingNeighbourNotification;
import protocols.broadcast.plumtree.notifications.SyncOpsNotification;
import protocols.broadcast.plumtree.requests.AddPendingToEagerRequest;
import protocols.broadcast.plumtree.requests.MyVectorClockRequest;
import protocols.broadcast.plumtree.requests.SendVectorClockRequest;
import protocols.broadcast.plumtree.requests.SyncOpsRequest;
import protocols.replication.notifications.*;
import protocols.replication.requests.*;
import protocols.replication.utils.OperationAndID;
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

    //CRDT Types
    private static final String CREATE_CRDT = "create";
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

    private final Host myself;
    private short broadcastId; //Broadcast protocol ID

    //Replication kernel variables
    private VectorClock vectorClock; //Local vector clock
    private int seqNumber; //Counter of local operation
    private Map<String, KernelCRDT> crdtsById; //Map that stores CRDTs by their ID
    private Map<String, Set<Host>> hostsByCrdt; //Map that stores the hosts that replicate a given CRDT
    public static List<OperationAndID> causallyOrderedOps; //List of causally ordered received operations

    //Serializers
    public static Map<String, MyOpSerializer> opSerializers = initializeOperationSerializers(); //Static map of operation serializers for each crdt type
    public Map<String, List<MySerializer>> dataSerializers; //Map of data type serializers by crdt ID

    //Debug variables
    public static int sentOps;
    public static int receivedOps;
    public static int executedOps;


    public ReplicationKernel(Properties properties, Host myself, short broadcastId) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.broadcastId = broadcastId;
        this.myself = myself;

        initializeVectorClock();
        this.crdtsById = new ConcurrentHashMap<>();
        this.hostsByCrdt = new ConcurrentHashMap<>();
        causallyOrderedOps = new ArrayList<>();

        this.dataSerializers = new HashMap<>();

        /* --------------------- Register Request Handlers --------------------- */
        registerRequestHandler(GetCRDTRequest.REQUEST_ID, this::uponGetCRDTRequest);
        registerRequestHandler(ReleaseCRDTRequest.REQUEST_ID, this::uponReleaseCRDTRequest);
        registerRequestHandler(DownstreamRequest.REQUEST_ID, this::uponDownstreamRequest);

        /* --------------------- Register Notification Handlers --------------------- */
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliverNotification);
        subscribeNotification(NewPendingNeighbourNotification.NOTIFICATION_ID, this::uponNewPendingNeighbourNotification);
        subscribeNotification(VectorClockNotification.NOTIFICATION_ID, this::uponVectorClockNotification);
        subscribeNotification(SendVectorClockNotification.NOTIFICATION_ID, this::uponSendVectorClockNotification);
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
            if (crdt != null) {
                if (validateCrdtType(crdt, crdtType)) {
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
     *
     * @param request
     * @param sourceProto
     */
    private void uponDownstreamRequest(DownstreamRequest request, short sourceProto) {
        UUID msgId = request.getMsgId();
        logger.debug("Received downstream request: {}", msgId);

        Operation op = request.getOperation();
        incrementAndSetVectorClock(op);
        causallyOrderedOps.add(new OperationAndID(op, msgId));
        try {
            broadcastOperation(false, msgId, request.getSender(), op);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /* --------------------------------- Notifications --------------------------------- */

    /**
     * Processes operations received from other replication kernels.
     *
     * @param notification
     * @param sourceProto
     */
    private void uponDeliverNotification(DeliverNotification notification, short sourceProto) {
        Host sender = notification.getSender();
        UUID msgId = notification.getMsgId();
        try {
            if (!sender.equals(myself)) {
                Operation op = deserializeOperation(notification.getMsg());
                Host h = op.getSender();
                int clock = op.getSenderClock();
                if (this.vectorClock.getHostClock(h) == clock - 1) {
                    logger.info("Accepted op {}-{} : {} from {}, Clock {}",
                            h, clock, notification.getMsgId(), sender, vectorClock.getHostClock(h));
                    executeOperation(h, op, msgId);
                } else if (this.vectorClock.getHostClock(h) < clock - 1) {
                    logger.error("Out-of-order op {}-{} : {} from {}, Clock {}",
                            h, clock, notification.getMsgId(), sender, vectorClock.getHostClock(h));
                } else {
                    logger.info("Ignored old op {}-{} : {} from {}, Clock {}",
                            h, clock, notification.getMsgId(), sender, vectorClock.getHostClock(h));
                }
            } else {
                executedOps++;
                sentOps++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponNewPendingNeighbourNotification(NewPendingNeighbourNotification notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        logger.info("Received {} with {}", notification, neighbour);
        sendRequest(new SendVectorClockRequest(UUID.randomUUID(), myself, neighbour), broadcastId);
    }

    private void uponVectorClockNotification(VectorClockNotification notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        logger.info("Received {}", notification);
        try {
            sendMissingSyncOperations(neighbour, notification.getVectorClock());
        } catch (IOException e) {
            e.printStackTrace();
        }
        sendRequest(new AddPendingToEagerRequest(UUID.randomUUID(), neighbour), broadcastId);
    }

    private void uponSendVectorClockNotification(SendVectorClockNotification notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        MyVectorClockRequest request = new MyVectorClockRequest(UUID.randomUUID(), myself, neighbour, this.vectorClock);
        logger.info("Sent {} to {}", request, neighbour);
        sendRequest(request, broadcastId);
    }

    private void uponSyncOpsNotification(SyncOpsNotification notification, short sourceProto) {
        //logger.info("Received {} from {}", notification, notification.getNeighbour());
        logger.info("SyncOpsNotification from {}, size {}", notification.getNeighbour(), notification.getOperations().size());
        try {

            Iterator<byte[]> opIt = notification.getOperations().iterator();
            Iterator<byte[]> idIt = notification.getIds().iterator();

            //Execute operations from synchronization
            while (opIt.hasNext() && idIt.hasNext()) {
                byte[] serOp = opIt.next();
                byte[] serId = idIt.next();
                Operation op = deserializeOperation(serOp);
                UUID msgId = deserializeId(serId);

//                logger.info("Sync operation with {}: {}", notification.getNeighbour(), op);
                Host h = op.getSender();
                int clock = op.getSenderClock();
                if (this.vectorClock.getHostClock(h) == clock - 1) {
                    logger.info("Sync op {}-{} : {} from {}, Clock {}",
                            h, clock, msgId, notification.getNeighbour(), vectorClock.getHostClock(h));
//                    executeOperation(h, op, msgId);
                    sendRequest(new BroadcastRequest(msgId, h, serOp), broadcastId);
                } else if (this.vectorClock.getHostClock(h) < clock - 1) {
                    logger.error("Sync Out-of-order op {}-{} : {} from {}, Clock {}",
                            h, clock, msgId, notification.getNeighbour(), vectorClock.getHostClock(h));
                } else {
                    logger.info("Sync Ignored old op {}-{} : {} from {}, Clock {}",
                            h, clock, msgId, notification.getNeighbour(), vectorClock.getHostClock(h));
                }

            }

        } catch (IOException e) {
            logger.error("EXCEPTION: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    /* --------------------------------- Interface Methods --------------------------------- */

    public void downstream(DownstreamRequest request, short sourceProto) {
        Operation op = request.getOperation();
        sendRequest(new DownstreamRequest(request.getMsgId(), request.getSender(), op), PROTOCOL_ID);
    }


    /* --------------------------------- Auxiliary Methods --------------------------------- */

    private void sendMissingSyncOperations(Host neighbour, VectorClock neighbourClock) throws IOException {
        int index = 0;
        for (OperationAndID opAndId : causallyOrderedOps) {
            Operation op = opAndId.getOp();
            Host h = op.getSender();
            int opClock = op.getSenderClock();
//            logger.info("{}-{} < {}", h, neighbourClock.getHostClock(h), opClock);
            if (neighbourClock.getHostClock(h) < opClock)
                break;
            index++;
        }

        List<byte[]> ops = new LinkedList<>();
        List<byte[]> ids = new LinkedList<>();
        for (int i = index; i < causallyOrderedOps.size(); i++) {
            OperationAndID opAndId = causallyOrderedOps.get(i);
            Operation op = opAndId.getOp();
            byte[] serializedOp = serializeOperation(op instanceof CreateOperation, op);
            ops.add(serializedOp);
            UUID id = opAndId.getId();
            byte[] serializedId = serializeId(id);
            ids.add(serializedId);
        }
        sendRequest(new SyncOpsRequest(UUID.randomUUID(), myself, neighbour, ids, ops), broadcastId);
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
        if (isCreateOp) {
            CreateOperation.serializer.serialize((CreateOperation) op, null, buf);
        } else {
            MySerializer[] serializers = dataSerializers.get(op.getCrdtId()).toArray(new MySerializer[2]);
            opSerializers.get(op.getCrdtType()).serialize(op, serializers, buf);
        }
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);
        return payload;
    }

    private byte[] serializeId(UUID id) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeLong(id.getMostSignificantBits());
        buf.writeLong(id.getLeastSignificantBits());
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);
        return payload;
    }

    private UUID deserializeId(byte[] msg) {
        ByteBuf buf = Unpooled.buffer().writeBytes(msg);
        long firstLong = buf.readLong();
        long secondLong = buf.readLong();
        return new UUID(firstLong, secondLong);
    }

    private void broadcastOperation(boolean isCreateOp, UUID msgId, Host sender, Operation op) throws IOException {
        byte[] payload = serializeOperation(isCreateOp, op);
        sendRequest(new BroadcastRequest(msgId, sender, payload), broadcastId);
    }

    private void executeOperation(Host sender, Operation op, UUID msgId) throws IOException {
        receivedOps++;
        causallyOrderedOps.add(new OperationAndID(op, msgId));

        String crdtId = op.getCrdtId();
        String crdtType = op.getCrdtType();

        if (op instanceof CreateOperation) {
            if (crdtsById.get(crdtId) == null) {
                createNewCrdt(crdtId, crdtType, ((CreateOperation) op).getDataTypes(), sender);
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
        CreateOperation op = new CreateOperation(null, 0, CREATE_CRDT, crdtId, crdtType, dataTypes);
        incrementAndSetVectorClock(op);
        causallyOrderedOps.add(new OperationAndID(op, msgId));
        broadcastOperation(true, msgId, sender, op);
    }

    /**
     * Maps each CRDT to its own dataType serializer. If the CRDT is a map
     * two serializers must be added (the first for the key, the second for
     * the value).
     *
     * @param crdtId    - ID of the CRDT.
     * @param crdtType  - type of the CRDT.
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
        if (crdtType.equals(OR_MAP))
            addExtraDataSerializerForMap(crdtId, dataTypes);
    }

    /**
     * Maps the second serializer (value serializer) for map CRDTs.
     *
     * @param crdtId    - ID of the CRDT.
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
     *
     * @param crdt     - CRDT to validate.
     * @param crdtType - type to check against.
     * @return true if the crdt is of crdtType, false otherwise.
     */
    private boolean validateCrdtType(KernelCRDT crdt, String crdtType) {
        switch (crdtType) {
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
     *
     * @param crdtId    - ID of the CRDT.
     * @param crdtType  - type of the CRDT.
     * @param dataTypes - data types of the CRDT.
     * @param sender    - host that created the CRDT.
     * @return the new crdt.
     */
    private KernelCRDT createNewCrdt(String crdtId, String crdtType, String[] dataTypes, Host sender) {
        KernelCRDT crdt;
        switch (crdtType) {
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
     *
     * @param crdtId - ID of the CRDT.
     * @param host   - host to add to the replication set.
     */
    private void addHostToReplicationSet(String crdtId, Host host) {
        Set<Host> replicas = hostsByCrdt.get(crdtId);
        if (replicas != null)
            replicas.add(host);
        else {
            replicas = new HashSet<>();
            replicas.add(host);
            hostsByCrdt.put(crdtId, replicas);
        }
    }

    /**
     * Removes the host from the replication set of the crdt with the given ID.
     *
     * @param crdtId - ID of the CRDT.
     * @param host   - host to remove from the replication set.
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
        logger.debug("Set local vc on my pos to {}", this.vectorClock.getHostClock(myself));

    }

    /**
     * Creates a map with the operation serializers for each crdt type.
     *
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

}
