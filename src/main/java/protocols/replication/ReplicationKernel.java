package protocols.replication;

import crdts.operations.*;
import datatypes.*;
import exceptions.NoSuchCrdtType;
import exceptions.NoSuchDataType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.notifications.InstallStateNotification;
import protocols.broadcast.common.notifications.SendStateNotification;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.common.requests.StateRequest;
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
    private final Map<String, String> crdtTypesById; //Map that stores CRDT Types by their ID
    private final Map<String, List<String>> dataTypesById; //Map that stores CRDT data types by their ID

    //Serializers
    public static Map<String, MyCRDTSerializer> crdtSerializers = initializeCDRTSerializers(); //Static map of CRDT serializers for each crdt type
    public static Map<String, MyOpSerializer> opSerializers = initializeOperationSerializers(); //Static map of operation serializers for each crdt type
    public Map<String, List<MySerializer>> dataSerializers; //Map of data type serializers by crdt ID

    public ReplicationKernel(short broadcastId) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.broadcastId = broadcastId;

        this.crdtsById = new ConcurrentHashMap<>();
        this.crdtTypesById = new HashMap<>();
        this.dataTypesById = new HashMap<>();

        this.dataSerializers = new HashMap<>();

        /* --------------------- Register Request Handlers --------------------- */
        registerRequestHandler(GetCRDTRequest.REQUEST_ID, this::uponGetCRDTRequest);
        registerRequestHandler(DownstreamRequest.REQUEST_ID, this::uponDownstreamRequest);
        registerRequestHandler(CounterOperationRequest.REQUEST_ID, this::uponCounterOperationRequest);
        registerRequestHandler(RegisterOperationRequest.REQUEST_ID, this::uponRegisterOperationRequest);
        registerRequestHandler(SetOperationRequest.REQUEST_ID, this::uponSetOperationRequest);
        registerRequestHandler(MapOperationRequest.REQUEST_ID, this::uponMapOperationRequest);

        /* --------------------- Register Notification Handlers --------------------- */
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliverNotification);
        subscribeNotification(SendStateNotification.NOTIFICATION_ID, this::uponSendStateNotification);
        subscribeNotification(InstallStateNotification.NOTIFICATION_ID, this::uponInstallStateNotification);

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

            KernelCRDT crdt = crdtsById.get(crdtId);
            if (crdt != null) {
                if (validateCrdtType(crdt, crdtType)) {
                    triggerNotification(new ReturnCRDTNotification(msgId, sender, crdt));
                } else {
                    triggerNotification(new CRDTAlreadyExistsNotification(msgId, sender, crdtId));
                }
            } else {
                crdt = createNewCrdt(crdtId, crdtType, dataTypes);
                triggerNotification(new ReturnCRDTNotification(msgId, sender, crdt));
                CreateOperation op = new CreateOperation(CREATE_CRDT, crdtId, crdtType, dataTypes);
                sendRequest(new BroadcastRequest(msgId, sender, serializeOperation(true, op)), broadcastId);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponDownstreamRequest(DownstreamRequest request, short sourceProto) {
        try {
            sendRequest(new BroadcastRequest(request.getMsgId(), request.getSender(),
                    serializeOperation(false, request.getOperation())), broadcastId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponCounterOperationRequest(CounterOperationRequest request, short sourceProto) {
        String crdtId = request.getCrdtId();
        KernelCRDT crdt = crdtsById.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof OpCounterCRDT) {
                Host sender = request.getSender();
                int value = request.getValue();
                OpCounterCRDT.CounterOpType opType = request.getOpType();
                switch(opType) {
                    case INCREMENT:
                        ((OpCounterCRDT) crdt).increment(sender);
                        break;
                    case DECREMENT:
                        ((OpCounterCRDT) crdt).decrement(sender);
                        break;
                    case INCREMENT_BY:
                        ((OpCounterCRDT) crdt).incrementBy(sender, value);
                        break;
                    case DECREMENT_BY:
                        ((OpCounterCRDT) crdt).decrementBy(sender, value);
                        break;
                    default:
                        //No other ops
                        break;
                }
            } else {
                //CRDT with crdtId is not a counterCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private void uponRegisterOperationRequest(RegisterOperationRequest request, short sourceProto) {
        String crdtId = request.getCrdtId();
        KernelCRDT crdt = crdtsById.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof LWWRegisterCRDT) {
                Host sender = request.getSender();
                SerializableType value = request.getValue();
                LWWRegisterCRDT.RegisterOpType opType = request.getOpType();
                switch(opType) {
                    case ASSIGN:
                        ((LWWRegisterCRDT) crdt).assign(sender, value);
                        break;
                    default:
                        //No other ops
                        break;
                }
            } else {
                //CRDT with crdtId is not a registerCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private void uponSetOperationRequest(SetOperationRequest request, short sourceProto) {
        String crdtId = request.getCrdtId();
        KernelCRDT crdt = crdtsById.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof ORSetCRDT) {
                Host sender = request.getSender();
                SerializableType value = request.getValue();
                ORSetCRDT.SetOpType opType = request.getOpType();
                switch(opType) {
                    case ADD:
                        ((ORSetCRDT) crdt).add(sender, value);
                        break;
                    case REMOVE:
                        ((ORSetCRDT) crdt).remove(sender, value);
                        break;
                    default:
                        //No other ops
                        break;
                }
            } else {
                //CRDT with crdtId is not a orSetCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }

    private void uponMapOperationRequest(MapOperationRequest request, short sourceProto) {
        String crdtId = request.getCrdtId();
        KernelCRDT crdt = crdtsById.get(crdtId);

        if(crdt != null) {
            if(crdt instanceof ORMapCRDT) {
                Host sender = request.getSender();
                SerializableType key = request.getKey();
                SerializableType value = request.getValue();
                ORMapCRDT.MapOpType opType = request.getOpType();
                switch(opType) {
                    case PUT:
                        ((ORMapCRDT) crdt).put(sender, key, value);
                        break;
                    case DELETE:
                        ((ORMapCRDT) crdt).delete(sender, key);
                        break;
                    default:
                        //No other ops
                        break;
                }
            } else {
                //CRDT with crdtId is not a orMapCRDT
            }
        } else {
            //No CRDT with crdtId
        }
    }


    /* --------------------------------- Notifications --------------------------------- */

    private void uponDeliverNotification(DeliverNotification notification, short sourceProto) {
        try {
            Operation op = deserializeOperation(notification.getMsg());
            String crdtId = op.getCrdtId();
            if (op instanceof CreateOperation) {
                if (crdtsById.get(crdtId) == null) {
                    createNewCrdt(crdtId,  op.getCrdtType(), ((CreateOperation) op).getDataTypes());
                }
            } else {
                crdtsById.get(crdtId).upstream(op);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponSendStateNotification(SendStateNotification notification, short sourceProto) {
        try {
            sendRequest(new StateRequest(notification.getMsgId(), serializeCurrentState()), broadcastId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void uponInstallStateNotification(InstallStateNotification notification, short sourceProto) {
        try {
            deserializeAndInstallState(notification.getState());
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

    private byte[] serializeCurrentState() throws IOException {

        ByteBuf buf = Unpooled.buffer();
        buf.writeInt(crdtsById.size()); //number of crdts
        for(Map.Entry<String, KernelCRDT> entry : crdtsById.entrySet()) {
            String crdtId = entry.getKey();
            String crdtType = crdtTypesById.get(crdtId);
            if(crdtType == null)
                logger.error("crdtType was null");
            List<String> dataTypes = dataTypesById.get(crdtId);
            buf.writeInt(crdtId.length());
            buf.writeBytes(crdtId.getBytes());
            buf.writeInt(crdtType.length());
            buf.writeBytes(crdtType.getBytes()); //crdtType (counter, register, etc)
            for (String dataType: dataTypes) { //dataTypes
                buf.writeInt(dataType.length());
                buf.writeBytes(dataType.getBytes());
            }
            MySerializer[] serializers = dataSerializers.get(crdtId).toArray(new MySerializer[2]);
            if(!crdtSerializers.containsKey(crdtType))
                logger.error("crdt serializer did not have crdtType {}", crdtType);
            crdtSerializers.get(crdtType).serialize(entry.getValue(), serializers, buf); // crdt itself
        }
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);
        return payload;
    }

    private void deserializeAndInstallState(byte[] state) throws IOException {
        ByteBuf buf = Unpooled.buffer().writeBytes(state);
        int nCRDTs = buf.readInt();
        for(int i = 0; i < nCRDTs; i++) {
            int size = buf.readInt();
            byte[] crdtIdArray = new byte[size];
            buf.readBytes(crdtIdArray);
            String crdtId = new String(crdtIdArray);
            size = buf.readInt();
            byte[] crdtTypeArray = new byte[size];
            buf.readBytes(crdtTypeArray);
            String crdtType = new String(crdtTypeArray);
            String[] dataTypes = new String[2];
            size = buf.readInt();
            byte[] dataTypeArray = new byte[size];
            buf.readBytes(dataTypeArray);
            dataTypes[0] = new String(dataTypeArray);
            if(crdtType.equals(OR_MAP)) {
                size = buf.readInt();
                dataTypeArray = new byte[size];
                buf.readBytes(dataTypeArray);
                dataTypes[1] = new String(dataTypeArray);
            }
            KernelCRDT crdt = crdtsById.get(crdtId);
            if(crdt == null)
                crdt = createNewCrdt(crdtId, crdtType, dataTypes);

            MySerializer[] serializers = dataSerializers.get(crdtId).toArray(new MySerializer[2]);
            KernelCRDT newCRDT = (KernelCRDT) crdtSerializers.get(crdtType).deserialize(this, serializers, buf);
            crdt.installState(newCRDT);
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
        List<String> dataTypeList = new ArrayList<>(2);

        switch (dataTypes[0]) {
            case INTEGER:
                serializerList.add(0, IntegerType.serializer);
                dataTypeList.add(0, INTEGER);
                break;
            case SHORT:
                serializerList.add(0, ShortType.serializer);
                dataTypeList.add(0, SHORT);
                break;
            case LONG:
                serializerList.add(0, LongType.serializer);
                dataTypeList.add(0, LONG);
                break;
            case FLOAT:
                serializerList.add(0, FloatType.serializer);
                dataTypeList.add(0, FLOAT);
                break;
            case DOUBLE:
                serializerList.add(0, DoubleType.serializer);
                dataTypeList.add(0, DOUBLE);
                break;
            case STRING:
                serializerList.add(0, StringType.serializer);
                dataTypeList.add(0, STRING);
                break;
            case BOOLEAN:
                serializerList.add(0, BooleanType.serializer);
                dataTypeList.add(0, BOOLEAN);
                break;
            case BYTE:
                serializerList.add(0, ByteType.serializer);
                dataTypeList.add(0, BYTE);
                break;
            default:
                throw new NoSuchDataType(dataTypes[0]);
        }
        dataSerializers.put(crdtId, serializerList);
        dataTypesById.put(crdtId, dataTypeList);
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
                dataTypesById.get(crdtId).add(1, INTEGER);
                break;
            case SHORT:
                dataSerializers.get(crdtId).add(1, ShortType.serializer);
                dataTypesById.get(crdtId).add(1, SHORT);
                break;
            case LONG:
                dataSerializers.get(crdtId).add(1, LongType.serializer);
                dataTypesById.get(crdtId).add(1, LONG);
                break;
            case FLOAT:
                dataSerializers.get(crdtId).add(1, FloatType.serializer);
                dataTypesById.get(crdtId).add(1, FLOAT);
                break;
            case DOUBLE:
                dataSerializers.get(crdtId).add(1, DoubleType.serializer);
                dataTypesById.get(crdtId).add(1, DOUBLE);
                break;
            case STRING:
                dataSerializers.get(crdtId).add(1, StringType.serializer);
                dataTypesById.get(crdtId).add(1, STRING);
                break;
            case BOOLEAN:
                dataSerializers.get(crdtId).add(1, BooleanType.serializer);
                dataTypesById.get(crdtId).add(1, BOOLEAN);
                break;
            case BYTE:
                dataSerializers.get(crdtId).add(1, ByteType.serializer);
                dataTypesById.get(crdtId).add(1, BYTE);
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
                crdtTypesById.put(crdtId, COUNTER);
                break;
            case LWW_REGISTER:
                crdt = new LWWRegisterCRDT(this, crdtId);
                crdtTypesById.put(crdtId, LWW_REGISTER);
                break;
            case OR_SET:
                crdt = new ORSetCRDT(this, crdtId);
                crdtTypesById.put(crdtId, OR_SET);
                break;
            case OR_MAP:
                crdt = new ORMapCRDT(this, crdtId);
                crdtTypesById.put(crdtId, OR_MAP);
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

    private static Map<String, MyCRDTSerializer> initializeCDRTSerializers() {
        Map<String, MyCRDTSerializer> map = new HashMap<>();
        map.put(COUNTER, OpCounterCRDT.serializer);
        map.put(LWW_REGISTER, LWWRegisterCRDT.serializer);
        map.put(OR_SET, ORSetCRDT.serializer);
        map.put(OR_MAP, ORMapCRDT.serializer);
        return map;
    }


}
