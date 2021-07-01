package protocols.replication;

import crdts.operations.*;
import crdts.operations.vc.*;
import crdts.utils.VectorClock;
import datatypes.*;
import exceptions.NoSuchCrdtType;
import exceptions.NoSuchDataType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.common.notifications.SendVectorClockNotification;
import protocols.broadcast.common.notifications.VectorClockNotification;
import protocols.broadcast.common.requests.SyncOpsRequest;
import protocols.broadcast.common.requests.VectorClockRequest;
import protocols.replication.notifications.CRDTAlreadyExistsNotification;
import protocols.replication.notifications.ReturnCRDTNotification;
import protocols.replication.requests.DownstreamRequest;
import protocols.replication.requests.GetCRDTRequest;
import protocols.replication.requests.ReleaseCRDTRequest;
import protocols.replication.utils.OperationAndID;
import protocols.replication.utils.SortOpsByHostClock;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;

public class ReplicationKernelVCs extends GenericProtocol implements CRDTCommunicationInterface {

    private static final Logger logger = LogManager.getLogger(ReplicationKernelVCs.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "ReplicationKernelVCs";
    public static final short PROTOCOL_ID = 1600;

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
    public static VectorClock vectorClock; //Local vector clock
    private int seqNumber; //Counter of local operation
    private Map<String, KernelCRDT> crdtsById; //Map that stores CRDTs by their ID
    private Map<String, Set<Host>> hostsByCrdt; //Map that stores the hosts that replicate a given CRDT

    private Map<Host, Queue<OperationAndID>> opsByHost;

    private File file;
    private int nExecuted;

    //Serializers
    public static Map<String, MyOpSerializer> opSerializers = initializeOperationSerializers(); //Static map of operation serializers for each crdt type
    public Map<String, List<MySerializer>> dataSerializers; //Map of data type serializers by crdt ID

    //Debug variables
    public static int sentOps;
    public static int receivedOps;
    public static int executedOps;


    public ReplicationKernelVCs(Properties properties, Host myself, short broadcastId) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.broadcastId = broadcastId;
        this.myself = myself;

        initializeVectorClock();
        this.crdtsById = new ConcurrentHashMap<>();
        this.hostsByCrdt = new ConcurrentHashMap<>();

        this.opsByHost = new ConcurrentHashMap<>();

        this.file = initFile();

        this.dataSerializers = new HashMap<>();

        /* --------------------- Register Request Handlers --------------------- */
        registerRequestHandler(GetCRDTRequest.REQUEST_ID, this::uponGetCRDT);
        registerRequestHandler(ReleaseCRDTRequest.REQUEST_ID, this::uponReleaseCRDT);
        registerRequestHandler(DownstreamRequest.REQUEST_ID, this::uponDownstream);

        /* --------------------- Register Notification Handlers --------------------- */
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliver);
        subscribeNotification(VectorClockNotification.NOTIFICATION_ID, this::uponVectorClock);
        subscribeNotification(SendVectorClockNotification.NOTIFICATION_ID, this::uponSendVectorClock);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for event from the application
    }


    /* --------------------------------- Requests --------------------------------- */

    private void uponGetCRDT(GetCRDTRequest request, short sourceProto) {
        try {
            String crdtId = request.getCrdtId();
            String crdtType = request.getCrdtType();
            String[] dataTypes = request.getDataType();
            Host sender = request.getSender();
            UUID msgId = request.getMsgId();

            KernelCRDT crdt = crdtsById.get(crdtId);
            if (crdt != null) {
                if (validateCrdtType(crdt, crdtType)) {
                    addHostToReplicationSet(crdtId, sender);
                    triggerNotification(new ReturnCRDTNotification(msgId, sender, crdt));
                } else {
                    triggerNotification(new CRDTAlreadyExistsNotification(msgId, sender, crdtId));
                }
            } else {
                handleCRDTCreation(crdtId, crdtType, dataTypes, sender, msgId);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponReleaseCRDT(ReleaseCRDTRequest request, short sourceProto) {
        String crdtId = request.getCrdtId();
        Host sender = request.getSender();
        removeHostFromReplicationSet(crdtId, sender);
    }

    /**
     * Propagates local operations to other replication kernels after incrementing
     * and setting the operation's vector clock.
     *
     * @param request -
     * @param sourceProto -
     */
    private void uponDownstream(DownstreamRequest request, short sourceProto) {
        UUID msgId = request.getMsgId();
        logger.info("GENERATED {}", msgId);
        logger.info("EXECUTED {}", msgId);
        logger.debug("Accepted my op {}-{} : {}", myself, seqNumber, msgId);

        OperationVC op = getOpWithVC(request.getOperation());
        incrementAndSetVectorClock(op);
        try {
            byte[] serOp = serializeOperation(false, op);
            writeOperationToFile(serOp, msgId);
            sendRequest(new BroadcastRequest(msgId, request.getSender(), serOp), broadcastId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private OperationVC getOpWithVC(Operation op) {
        if(op instanceof CounterOperation) {
            return new CounterOperationVC(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((CounterOperation) op).getValue(), null);
        } else if(op instanceof RegisterOperation) {
            return new RegisterOperationVC(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((RegisterOperation) op).getValue(), ((RegisterOperation) op).getTimestamp(), null);
        } else if(op instanceof SetOperation) {
            return new SetOperationVC(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((SetOperation) op).getSet(), null);
        } else if(op instanceof MapOperation) {
            return new MapOperationVC(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((MapOperation) op).getKey(), ((MapOperation) op).getElement(),
                    ((MapOperation) op).getSet(), null);
        } else {
            return null;
        }
    }

    /* --------------------------------- Notifications --------------------------------- */

    /**
     * Processes operations received from other replication kernels.
     *
     * @param notification -
     * @param sourceProto -
     */
    private void uponDeliver(DeliverNotification notification, short sourceProto) {
        Host sender = notification.getSender();
        UUID msgId = notification.getMsgId();
        byte[] serOp = notification.getMsg();
        try {
            if (!sender.equals(myself)) {
                logger.debug("Deserializing {} received from {}", msgId, sender);
                OperationVC op = deserializeOperation(serOp);
                Host h = op.getSender();

                if (this.vectorClock.canExecuteOperation(op)) {
                    logger.debug("Executing {} without queueing", msgId);
                    writeOperationToFile(serOp, msgId);
                    executeOperation(h, op, msgId);
                    executeQueuedOperations();
                } else {
                    logger.debug("Adding {} to queue", msgId);
                    addOperationToQueue(h, op, msgId, op instanceof CreateOperationVC);
                }

            } else {
                executedOps++;
                sentOps++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void executeQueuedOperations() throws IOException {
        for (Map.Entry<Host, Queue<OperationAndID>> entry : opsByHost.entrySet()) {
            Host h = entry.getKey();
            Queue<OperationAndID> q = entry.getValue();
            OperationAndID opAndId = q.peek();
            while(opAndId != null && this.vectorClock.canExecuteOperation(opAndId.getOp())) {
                opAndId = q.remove();
                OperationVC op = opAndId.getOp();
                UUID id = opAndId.getId();
                logger.debug("Executing queued {}", id);
                writeOperationToFile(serializeOperation(opAndId.isCreateOp(), op), id);
                executeOperation(h, op, id);
                opAndId = q.peek();
            }
        }
    }

    private void addOperationToQueue(Host sender, OperationVC op, UUID msgId, boolean isCreateOp) {
        Queue<OperationAndID> queue = opsByHost.get(sender);
        OperationAndID opAndId = new OperationAndID(op, msgId, isCreateOp);
        if(queue != null) {
            queue.add(opAndId);
        } else {
            queue = new PriorityBlockingQueue<>(500, new SortOpsByHostClock());
            queue.add(opAndId);
            opsByHost.put(sender, queue);
        }
    }

    private void uponVectorClock(VectorClockNotification notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        logger.debug("Received {}", notification);
        try {
            readAndSendMissingOpsFromFile(neighbour,  notification.getVectorClock());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponSendVectorClock(SendVectorClockNotification notification, short sourceProto) {
        Host neighbour = notification.getNeighbour();
        VectorClockRequest request = new VectorClockRequest(UUID.randomUUID(), myself, neighbour, new VectorClock(this.vectorClock.getClock()));
        logger.debug("Sent {} to {}", request, neighbour);
        sendRequest(request, broadcastId);
    }

    /* --------------------------------- Interface Methods --------------------------------- */

    public void downstream(DownstreamRequest request, short sourceProto) {
        Operation op = request.getOperation();
        sendRequest(new DownstreamRequest(request.getMsgId(), request.getSender(), op), PROTOCOL_ID);
    }


    /* --------------------------------- Procedures --------------------------------- */

    private OperationVC deserializeOperation(byte[] msg) throws IOException {
        ByteBuf buf = Unpooled.buffer().writeBytes(msg);
        String crdtId = OperationVC.crdtIdFromByteArray(buf);
        String crdtType = OperationVC.crdtTypeFromByteArray(buf);
        String opType = OperationVC.opTypeFromByteArray(buf);
        OperationVC op;

        if (opType.equals(CREATE_CRDT)) {
            op = CreateOperationVC.serializer.deserialize(null, buf);
        } else {
            List<MySerializer> l = dataSerializers.get(crdtId);
            if(l == null) {
                logger.debug("CRDT with id {} does not exist", crdtId);
            }
            MySerializer[] serializers = dataSerializers.get(crdtId).toArray(new MySerializer[2]);
            op = (OperationVC) opSerializers.get(crdtType).deserialize(serializers, buf);
        }
        return op;
    }

    private byte[] serializeOperation(boolean isCreateOp, OperationVC op) throws IOException {
        ByteBuf buf = Unpooled.buffer();
        if (isCreateOp) {
            CreateOperationVC.serializer.serialize((CreateOperationVC) op, null, buf);
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

    private void executeOperation(Host sender, OperationVC op, UUID msgId) throws IOException {
        String crdtId = op.getCrdtId();
        String crdtType = op.getCrdtType();

        if (op instanceof CreateOperationVC) {
            if (crdtsById.get(crdtId) == null) {
                createNewCrdt(crdtId, crdtType, ((CreateOperationVC) op).getDataTypes(), sender);
                logger.debug("Created {} {}", crdtId, msgId);
            } else {
                addHostToReplicationSet(crdtId, sender);
            }
        } else {
            crdtsById.get(crdtId).upstream(getOpWithoutVC(op));
        }
        this.vectorClock.incrementClock(sender);
        logger.info("EXECUTED {}", msgId);
        executedOps++;
        receivedOps++;
    }

    private Operation getOpWithoutVC(OperationVC op) {
        if(op instanceof CounterOperationVC) {
            return new CounterOperation(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((CounterOperationVC) op).getValue());
        } else if(op instanceof RegisterOperationVC) {
            return new RegisterOperation(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((RegisterOperationVC) op).getValue(), ((RegisterOperationVC) op).getTimestamp());
        } else if(op instanceof SetOperationVC) {
            return new SetOperation(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((SetOperationVC) op).getSet());
        } else if(op instanceof MapOperationVC) {
            return new MapOperation(op.getSender(), op.getSenderClock(), op.getOpType(), op.getCrdtId(),
                    op.getCrdtType(), ((MapOperationVC) op).getKey(), ((MapOperationVC) op).getElement(),
                    ((MapOperationVC) op).getSet());
        } else {
            return null;
        }
    }

    private void handleCRDTCreation(String crdtId, String crdtType, String[] dataTypes, Host sender, UUID msgId) throws IOException {
        logger.debug("Created {}; Accepted my op {}-{} : {}", crdtId, myself, seqNumber, msgId);
        logger.info("GENERATED {}", msgId);
        KernelCRDT crdt = createNewCrdt(crdtId, crdtType, dataTypes, sender);
        triggerNotification(new ReturnCRDTNotification(msgId, sender, crdt));
        logger.info("EXECUTED {}", msgId);
        CreateOperationVC op = new CreateOperationVC(null, 0, CREATE_CRDT, crdtId, crdtType, dataTypes, null);
        incrementAndSetVectorClock(op);
        byte[] serOp = serializeOperation(true, op);
        writeOperationToFile(serOp, msgId);
        sendRequest(new BroadcastRequest(msgId, sender, serOp), broadcastId);
    }

    private void writeOperationToFile(byte[] serOp, UUID msgId) throws IOException {
        try(FileOutputStream fos = new FileOutputStream(this.file, true);
            DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeLong(msgId.getMostSignificantBits());
            dos.writeLong(msgId.getLeastSignificantBits());
            dos.writeInt(serOp.length);
            if (serOp.length > 0) {
                dos.write(serOp);
            }
            nExecuted++;
        } catch (IOException e) {
            logger.error("Error writing ops to file", e);
            e.printStackTrace();
        }
    }

    private void readAndSendMissingOpsFromFile(Host neighbour, VectorClock neighbourClock) throws IOException {
        long startTime = System.currentTimeMillis();
        List<byte[]> ops = new LinkedList<>();
        List<byte[]> ids = new LinkedList<>();
        try (FileInputStream fis = new FileInputStream(this.file);
             BufferedInputStream bis = new BufferedInputStream(fis);
             DataInputStream dis = new DataInputStream(bis)) {

            for (int i = 0; i < nExecuted; i++) {
                long firstLong = dis.readLong();
                long secondLong = dis.readLong();
                UUID msgId = new UUID(firstLong, secondLong);
                int size = dis.readInt();
                if (size > 0) {
                    byte[] serOp = new byte[size];
                    dis.read(serOp, 0, size);
                    Operation op = deserializeOperation(serOp);
                    Host h = op.getSender();
                    int opClock = op.getSenderClock();
                    if (neighbourClock.getHostClock(h) < opClock) {
                        ops.add(serOp);
                        byte[] serializedId = serializeId(msgId);
                        ids.add(serializedId);
                    }
                }
            }
            long endTime = System.currentTimeMillis();
            logger.info("READ FROM FILE in {} ms", endTime - startTime);
            sendRequest(new SyncOpsRequest(UUID.randomUUID(), myself, neighbour, ids, ops), broadcastId);
        } catch (IOException e) {
            logger.error("Error reading missing ops from file", e);
            e.printStackTrace();
        }
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

    private void incrementAndSetVectorClock(OperationVC op) {
        this.vectorClock.incrementClock(myself);
        op.setSender(myself);
        op.setSenderClock(++seqNumber);
        op.setVectorClock(new VectorClock(this.vectorClock.getClock()));
    }

    /**
     * Creates a map with the operation serializers for each crdt type.
     *
     * @return the created map.
     */
    private static Map<String, MyOpSerializer> initializeOperationSerializers() {
        Map<String, MyOpSerializer> map = new HashMap<>();
        map.put(COUNTER, CounterOperationVC.serializer);
        map.put(LWW_REGISTER, RegisterOperationVC.serializer);
        map.put(OR_SET, SetOperationVC.serializer);
        map.put(OR_MAP, MapOperationVC.serializer);
        return map;
    }

    private File initFile() throws IOException {
        File file = new File("data/ops-" + myself);
        file.createNewFile();
        new FileOutputStream("data/ops-" + myself).close();
        return file;
    }
}