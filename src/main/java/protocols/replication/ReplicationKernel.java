package protocols.replication;

import crdts.interfaces.CounterCRDT;
import crdts.operations.*;
import datatypes.*;
import exceptions.NoSuchCrdtType;
import exceptions.NoSuchDataType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.replication.notifications.*;
import protocols.replication.requests.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;
import serializers.MyCRDTSerializer;
import serializers.MyOpSerializer;
import serializers.MySerializer;

import java.io.*;
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

    private final short broadcastId; //Broadcast protocol ID

    private final Map<String, KernelCRDT> crdtsById; //Map that stores CRDTs by their ID

    //Serializers
    //TODO: serializers de CRDTS
    public static Map<String, MyOpSerializer> crdtSerializers = initializeCRDTSerializers(); //Static map of CRDT serializers for each crdt type
    public static Map<String, MyOpSerializer> opSerializers = initializeOperationSerializers(); //Static map of operation serializers for each crdt type
    public Map<String, List<MySerializer>> dataSerializers; //Map of data type serializers by crdt ID

    public ReplicationKernel(short broadcastId) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.broadcastId = broadcastId;

        this.crdtsById = new ConcurrentHashMap<>();

        this.dataSerializers = new HashMap<>();

        /* --------------------- Register Request Handlers --------------------- */
        registerRequestHandler(GetCRDTRequest.REQUEST_ID, this::uponGetCRDT);
        registerRequestHandler(DownstreamRequest.REQUEST_ID, this::uponDownstream);

        /* --------------------- Register Notification Handlers --------------------- */
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliver);
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

    /**
     * Propagates local operations to other replication kernels after incrementing
     * and setting the operation's vector clock.
     *
     * @param request -
     * @param sourceProto -
     */
    private void uponDownstream(DownstreamRequest request, short sourceProto) {
        try {
            sendRequest(new BroadcastRequest(request.getMsgId(), request.getSender(),
                    serializeOperation(false, request.getOperation())), broadcastId);
        } catch (IOException e) {
            e.printStackTrace();
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
        try {
            executeOperation(deserializeOperation(notification.getMsg()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /* --------------------------------- Interface Methods --------------------------------- */

    /**
     * CRDTs call this method in order to communicate to the kernel that there is a downstream operation
     * @param request - request that contains the operation to propagate.
     * @param sourceProto -
     */
    public void downstream(DownstreamRequest request, short sourceProto) {
        sendRequest(new DownstreamRequest(request.getMsgId(), request.getSender(), request.getOperation()), PROTOCOL_ID);
    }


    /* --------------------------------- Procedures --------------------------------- */

    private void handleCRDTCreation(String crdtId, String crdtType, String[] dataTypes, Host sender, UUID msgId) throws IOException {
        KernelCRDT crdt = createNewCrdt(crdtId, crdtType, dataTypes);
        triggerNotification(new ReturnCRDTNotification(msgId, sender, crdt));
        CreateOperation op = new CreateOperation(CREATE_CRDT, crdtId, crdtType, dataTypes);
        sendRequest(new BroadcastRequest(msgId, sender, serializeOperation(true, op)), broadcastId);
    }

    private void executeOperation(Operation op) throws IOException {
        String crdtId = op.getCrdtId();

        if (op instanceof CreateOperation) {
            if (crdtsById.get(crdtId) == null) {
                createNewCrdt(crdtId,  op.getCrdtType(), ((CreateOperation) op).getDataTypes());
            }
        } else {
            crdtsById.get(crdtId).upstream(op);
        }
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
     * @return the new crdt.
     */
    private KernelCRDT createNewCrdt(String crdtId, String crdtType, String[] dataTypes) {
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
        registerDataSerializer(crdtId, crdtType, dataTypes);
        return crdt;
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

    /**
     * Creates a map with the CRDT serializers for each crdt type.
     *
     * @return the created map.
     */
    private static Map<String, MyCRDTSerializer> initializeCRDTSerializers() {
        Map<String, MyCRDTSerializer> map = new HashMap<>();
        map.put(COUNTER, CounterCRDT.serializer);
        map.put(LWW_REGISTER, LWWRegisterCRDT.serializer);
        map.put(OR_SET, ORSetCRDT.serializer);
        map.put(OR_MAP, ORMapCRDT.serializer);
        return map;
    }
}
