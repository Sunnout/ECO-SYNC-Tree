package protocols.apps;

import java.util.*;

import protocols.broadcast.flood.FloodBroadcast;
import protocols.broadcast.flood.utils.FloodStats;
import protocols.replication.crdts.interfaces.*;
import protocols.broadcast.periodicpull.PeriodicPullBroadcast;
import protocols.broadcast.plumtree.PlumTree;
import protocols.broadcast.plumtree.utils.PlumtreeStats;
import protocols.replication.OpCounterCRDT.CounterOpType;
import protocols.replication.LWWRegisterCRDT.RegisterOpType;
import protocols.replication.ORSetCRDT.SetOpType;
import protocols.replication.ORMapCRDT.MapOpType;
import protocols.replication.crdts.datatypes.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.apps.timers.*;
import protocols.replication.notifications.*;
import protocols.replication.requests.*;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

public class CRDTApp extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(CRDTApp.class);
    private static final int TO_MILLIS = 1000;

    //RUN = 0 --> counter; 1 --> register; 2 --> set; 3 --> map; 4 --> 8 registers;
    //5 --> 8 sets; 6 --> 8 maps; 7 --> 1 of each CRDT; 8 --> counter + register + set + map
    private static final int RUN = 8;

    private static final String COUNTER = "counter";
    private static final String LWW_REGISTER = "lww_register";
    private static final String OR_SET = "or_set";
    private static final String OR_MAP = "or_map";

    private static final String CRDT0 = "CRDT0";
    private static final String CRDT1 = "CRDT1";
    private static final String CRDT2 = "CRDT2";
    private static final String CRDT3 = "CRDT3";
    private static final String CRDT4 = "CRDT4";
    private static final String CRDT5 = "CRDT5";
    private static final String CRDT6 = "CRDT6";
    private static final String CRDT7 = "CRDT7";
    private static final String CRDT8 = "CRDT8";

    //Protocol information, to register in babel
    public static final String PROTO_NAME = "CRDTApp";
    public static final short PROTO_ID = 300;

    private final short replicationKernelId;
    private final short broadcastId;
    private final Host self;

    //Time to wait until creating protocols.replication.crdts
    private final int createTime;
    //Time to run before stopping sending messages
    private final int runTime;
    //Time to wait until printing final values
    private final int cooldownTime;
    //Time to wait until shut down
    private final int exitTime;

    private final float prob;

    //Interval between each increment
    private final int ops1Interval;

    //Increment(By), decrement(By) and value periodic timers
    private long ops1Timer;

    //Map of crdtId to GenericCRDT
    private final Map<String, GenericCRDT> myCRDTs;

    private final Random rand;

    public CRDTApp(Properties properties, Host self, short replicationKernelId, short broadcastId) throws HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.replicationKernelId = replicationKernelId;
        this.broadcastId = broadcastId;
        this.self = self;
        this.myCRDTs = new HashMap<>();

        //Read configurations
        this.createTime = Integer.parseInt(properties.getProperty("create_time"));
        this.runTime = Integer.parseInt(properties.getProperty("run_time"));
        this.cooldownTime = Integer.parseInt(properties.getProperty("cooldown_time"));
        this.exitTime = Integer.parseInt(properties.getProperty("exit_time"));
        this.ops1Interval = Integer.parseInt(properties.getProperty("ops1"));
        this.prob = Float.parseFloat(properties.getProperty("op_probability", "1"));

        this.rand = new Random();

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(ExecuteOps1Timer.TIMER_ID, this::uponExecuteOps1Timer);
        registerTimerHandler(CreateCRDTsTimer.TIMER_ID, this::uponCreateCRDTsTimer);
        registerTimerHandler(StopTimer.TIMER_ID, this::uponStopTimer);
        registerTimerHandler(PrintValuesTimer.TIMER_ID, this::uponPrintValuesTimer);
        registerTimerHandler(ExitTimer.TIMER_ID, this::uponExitTimer);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ReturnCRDTNotification.NOTIFICATION_ID, this::uponReturnCRDTNotification);
        subscribeNotification(CRDTAlreadyExistsNotification.NOTIFICATION_ID, this::uponCRDTAlreadyExistsNotification);

    }

    @Override
    public void init(Properties props) {
        //Wait before creating protocols.replication.crdts
        logger.info("Waiting...");
        setupTimer(new CreateCRDTsTimer(), (long) createTime * TO_MILLIS);
    }


    /* --------------------------------- Requests --------------------------------- */

    private void getCRDT(String crdtType, String[] dataType, String crdtId) {
        //Creating new CRDT by asking the replication kernel for it
        sendRequest(new GetCRDTRequest(UUID.randomUUID(), crdtType, dataType, crdtId), replicationKernelId);
    }

    private void executeCounterOperation(String crdtId, CounterOpType opType, int value) {
        sendRequest(new CounterOperationRequest(crdtId, opType, value), replicationKernelId);
    }

    private void executeRegisterOperation(String crdtId, RegisterOpType opType, SerializableType value) {
        sendRequest(new RegisterOperationRequest(crdtId, opType, value), replicationKernelId);
    }

    private void executeSetOperation(String crdtId, SetOpType opType, SerializableType value) {
        sendRequest(new SetOperationRequest(crdtId, opType, value), replicationKernelId);
    }

    private void executeMapOperation(String crdtId, MapOpType opType, SerializableType key, SerializableType value) {
        sendRequest(new MapOperationRequest(crdtId, opType, key, value), replicationKernelId);
    }


    /* --------------------------------- Notifications --------------------------------- */

    private void uponReturnCRDTNotification(ReturnCRDTNotification notification, short sourceProto) {
        GenericCRDT crdt = notification.getCrdt();
        String crdtId = crdt.getCrdtId();
        logger.info("CRDT {} was created by {} - {}", crdtId, self, notification.getMsgId());
        myCRDTs.put(crdtId, crdt);
    }

    private void uponCRDTAlreadyExistsNotification(CRDTAlreadyExistsNotification notification, short sourceProto) {
        logger.info("CRDT {} already exists for {}", notification.getCrdtId(), self);
    }


    /* --------------------------------- Timers --------------------------------- */

    private void uponCreateCRDTsTimer(CreateCRDTsTimer timer, long timerId) {
        logger.info("Creating protocols.replication.crdts...");
        getCRDTs();
        logger.warn("Starting operations...");
        ops1Timer = setupPeriodicTimer(new ExecuteOps1Timer(), 0, ops1Interval);
        setupTimer(new StopTimer(), (long) runTime * TO_MILLIS);
    }

    private void uponExecuteOps1Timer(ExecuteOps1Timer incTimer, long timerId) {
        executeWithProbability(prob);
    }

    private void uponStopTimer(StopTimer stopTimer, long timerId) {
        logger.warn("Stopping broadcasts");
        //Stop executing operations
        this.cancelTimer(ops1Timer);
        setupTimer(new PrintValuesTimer(), (long) cooldownTime * TO_MILLIS);
    }

    private void uponPrintValuesTimer(PrintValuesTimer printValuesTimer, long timerId) {
        printFinalValues();
        printStats();
//        sendRequest(new PrintStateRequest(), replicationKernelId);
        setupTimer(new ExitTimer(), (long) exitTime * TO_MILLIS);
    }

    private void uponExitTimer(ExitTimer exitTimer, long timerId) {
        logger.warn("Exiting...");
        System.exit(0);
    }


    /* --------------------------------- Auxiliary Methods --------------------------------- */

    private void getCRDTs() {
        if(CRDTApp.RUN == 0) {
            getCRDT(COUNTER, new String[]{"int"}, CRDT0);
        } else if(CRDTApp.RUN == 1) {
            getCRDT(LWW_REGISTER, new String[]{"int"}, CRDT1);
        } else if(CRDTApp.RUN == 2) {
            getCRDT(OR_SET, new String[]{"int"}, CRDT2);
        } else if(CRDTApp.RUN == 3) {
            getCRDT(OR_MAP, new String[]{"byte", "int"}, CRDT3);
        } else if(CRDTApp.RUN == 4) {
            getCRDT(LWW_REGISTER, new String[]{"int"}, CRDT1);
            getCRDT(LWW_REGISTER, new String[]{"long"}, CRDT2);
            getCRDT(LWW_REGISTER, new String[]{"short"}, CRDT3);
            getCRDT(LWW_REGISTER, new String[]{"float"}, CRDT4);
            getCRDT(LWW_REGISTER, new String[]{"double"}, CRDT5);
            getCRDT(LWW_REGISTER, new String[]{"string"}, CRDT6);
            getCRDT(LWW_REGISTER, new String[]{"boolean"}, CRDT7);
            getCRDT(LWW_REGISTER, new String[]{"byte"}, CRDT8);
        } else if(CRDTApp.RUN == 5) {
            getCRDT(OR_SET, new String[]{"int"}, CRDT1);
            getCRDT(OR_SET, new String[]{"long"}, CRDT2);
            getCRDT(OR_SET, new String[]{"short"}, CRDT3);
            getCRDT(OR_SET, new String[]{"float"}, CRDT4);
            getCRDT(OR_SET, new String[]{"double"}, CRDT5);
            getCRDT(OR_SET, new String[]{"string"}, CRDT6);
            getCRDT(OR_SET, new String[]{"boolean"}, CRDT7);
            getCRDT(OR_SET, new String[]{"byte"}, CRDT8);
        } else if(CRDTApp.RUN == 6) {
            getCRDT(OR_MAP, new String[]{"byte", "int"}, CRDT1);
            getCRDT(OR_MAP, new String[]{"byte", "short"}, CRDT2);
            getCRDT(OR_MAP, new String[]{"byte", "long"}, CRDT3);
            getCRDT(OR_MAP, new String[]{"byte", "float"}, CRDT4);
            getCRDT(OR_MAP, new String[]{"byte", "double"}, CRDT5);
            getCRDT(OR_MAP, new String[]{"byte", "boolean"}, CRDT6);
            getCRDT(OR_MAP, new String[]{"byte", "string"}, CRDT7);
            getCRDT(OR_MAP, new String[]{"byte", "byte"}, CRDT8);
        } else if(CRDTApp.RUN == 7) {
            getCRDT(COUNTER, new String[]{"int"}, CRDT0);
            getCRDT(LWW_REGISTER, new String[]{"int"}, CRDT1);
            getCRDT(OR_SET, new String[]{"int"}, CRDT2);
            getCRDT(OR_MAP, new String[]{"byte", "int"}, CRDT3);
        } else if(CRDTApp.RUN == 8) {
            getCRDT(COUNTER, new String[]{"int"}, CRDT0);
            getCRDT(LWW_REGISTER, new String[]{"int"}, CRDT1);
            getCRDT(OR_SET, new String[]{"int"}, CRDT2);
            getCRDT(OR_MAP, new String[]{"int", "int"}, CRDT3);
        }
    }

    private void executeWithProbability(double prob) {
        if(Math.random() <= prob) {
            executeCounterOperation(CRDT0, CounterOpType.INCREMENT, 1);
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(1000)));
            executeSetOperation(CRDT2, Math.random() > 0.5 ? SetOpType.ADD : SetOpType.REMOVE, new IntegerType(rand.nextInt(50)));
            executeMapOperation(CRDT3, MapOpType.PUT, new IntegerType(rand.nextInt(10)), new IntegerType(rand.nextInt(1000)));
        }
    }

    private void printFinalValues() {
        logger.warn("RESULTS:");
        if(broadcastId == PlumTree.PROTOCOL_ID) {
            logger.info("Final vector clock: {}", PlumTree.vectorClock);
        }

        if(CRDTApp.RUN == 0) {
            logger.info("Integer value of {}: {}", CRDT0, getCounterValue(CRDT0));
        } else if(CRDTApp.RUN == 1) {
            logger.info("Integer value of {}: {}", CRDT1, getRegisterValue(CRDT1));
        } else if(CRDTApp.RUN == 2) {
            logger.info("Value of {}: {}", CRDT2, getSetValue(CRDT2));
        } else if(CRDTApp.RUN == 3) {
            Set<SerializableType> keys = getMapKeys(CRDT3);
            for(SerializableType key : keys) {
                logger.info("{} key {} : {}", CRDT3, key, getMapping(CRDT3, key));
            }
            logger.info("Values of {}: {}", CRDT3, getMapValues(CRDT3));
        } else if(CRDTApp.RUN == 4) {
            logger.info("Integer value of {}: {}", CRDT1, getRegisterValue(CRDT1));
            logger.info("Long value of {}: {}", CRDT2, getRegisterValue(CRDT2));
            logger.info("Short value of {}: {}", CRDT3, getRegisterValue(CRDT3));
            logger.info("Float value of {}: {}", CRDT4, getRegisterValue(CRDT4));
            logger.info("Double value of {}: {}", CRDT5, getRegisterValue(CRDT5));
            logger.info("String value of {}: {}", CRDT6, getRegisterValue(CRDT6));
            logger.info("Boolean value of {}: {}", CRDT7, getRegisterValue(CRDT7));
            logger.info("Byte value of {}: {}", CRDT8, getRegisterValue(CRDT8));
        } else if(CRDTApp.RUN == 5) {
            logger.info("Value of {}: {}", CRDT1, getSetValue(CRDT1));
            logger.info("Value of {}: {}", CRDT2, getSetValue(CRDT2));
            logger.info("Value of {}: {}", CRDT3, getSetValue(CRDT3));
            logger.info("Value of {}: {}", CRDT4, getSetValue(CRDT4));
            logger.info("Value of {}: {}", CRDT5, getSetValue(CRDT5));
            logger.info("Value of {}: {}", CRDT6, getSetValue(CRDT6));
            logger.info("Value of {}: {}", CRDT7, getSetValue(CRDT7));
            logger.info("Value of {}: {}", CRDT8, getSetValue(CRDT8));
        } else if(CRDTApp.RUN == 6) {
            logger.info("Keys of {}: {}", CRDT1, getMapKeys(CRDT1));
            logger.info("Values of {}: {}", CRDT1, getMapValues(CRDT1));
            logger.info("Keys of {}: {}", CRDT2, getMapKeys(CRDT2));
            logger.info("Values of {}: {}", CRDT2, getMapValues(CRDT2));
            logger.info("Keys of {}: {}", CRDT3, getMapKeys(CRDT3));
            logger.info("Values of {}: {}", CRDT3, getMapValues(CRDT3));
            logger.info("Keys of {}: {}", CRDT4, getMapKeys(CRDT4));
            logger.info("Values of {}: {}", CRDT4, getMapValues(CRDT4));
            logger.info("Keys of {}: {}", CRDT5, getMapKeys(CRDT5));
            logger.info("Values of {}: {}", CRDT5, getMapValues(CRDT5));
            logger.info("Keys of {}: {}", CRDT6, getMapKeys(CRDT6));
            logger.info("Values of {}: {}", CRDT6, getMapValues(CRDT6));
            logger.info("Keys of {}: {}", CRDT7, getMapKeys(CRDT7));
            logger.info("Values of {}: {}", CRDT7, getMapValues(CRDT7));
            logger.info("Keys of {}: {}", CRDT8, getMapKeys(CRDT8));
            logger.info("Values of {}: {}", CRDT8, getMapValues(CRDT8));
        } else if(CRDTApp.RUN == 7) {
            logger.info("Integer value of {}: {}", CRDT0, getCounterValue(CRDT0));
            logger.info("Integer value of {}: {}", CRDT1, getRegisterValue(CRDT1));
            logger.info("Value of {}: {}", CRDT2, getSetValue(CRDT2));
            Set<SerializableType> keys = getMapKeys(CRDT3);
            for(SerializableType key : keys) {
                logger.info("{} key {} : {}", CRDT3, key, getMapping(CRDT3, key));
            }
            logger.info("Values of {}: {}", CRDT3, getMapValues(CRDT3));
        } else if(CRDTApp.RUN == 8) {
            logger.info("[CRDT-VAL] {} {}", CRDT0, getCounterValue(CRDT0));
            logger.info("[CRDT-VAL] {} {}", CRDT1, getRegisterValue(CRDT1));
            logger.info("[CRDT-VAL] {} {}", CRDT2, getSetValue(CRDT2));
            Set<SerializableType> keys = getMapKeys(CRDT3);
            for(SerializableType key : keys) {
                logger.info("[CRDT-VAL] {}:{} {}", CRDT3, key, getMapping(CRDT3, key));
            }
        }

        if(broadcastId == PlumTree.PROTOCOL_ID) {
            logger.info("Number of sent operations: {}", PlumtreeStats.sentOps);
            logger.info("Number of received operations: {}", PlumtreeStats.receivedOps);
            logger.warn("Number of executed operations: {}", PlumtreeStats.executedOps);
        }
    }

    private void printStats() {
        //Plumtree
        if(broadcastId == PlumTree.PROTOCOL_ID) {
            logger.info("sentGossip: {}", PlumtreeStats.sentGossip);
            logger.info("sentIHave: {}", PlumtreeStats.sentIHave);
            logger.info("sentGraft: {}", PlumtreeStats.sentGraft);
            logger.info("sentPrune: {}", PlumtreeStats.sentPrune);
            logger.info("sentSendVC: {}", PlumtreeStats.sentSendVC);
            logger.info("sentVC: {}", PlumtreeStats.sentVC);
            logger.info("sentSyncOps: {}", PlumtreeStats.sentSyncOps);
            logger.info("sentSyncGossip: {}", PlumtreeStats.sentSyncGossip);

            logger.info("receivedGossip: {}", PlumtreeStats.receivedGossip);
            logger.info("receivedDupesGossip: {}", PlumtreeStats.receivedDupesGossip);
            logger.info("receivedIHave: {}", PlumtreeStats.receivedIHave);
            logger.info("receivedGraft: {}", PlumtreeStats.receivedGraft);
            logger.info("receivedPrune: {}", PlumtreeStats.receivedPrune);
            logger.info("receivedSendVC: {}", PlumtreeStats.receivedSendVC);
            logger.info("receivedVC: {}", PlumtreeStats.receivedVC);
            logger.info("receivedSyncOps: {}", PlumtreeStats.receivedSyncOps);
            logger.info("receivedSyncGossip: {}", PlumtreeStats.receivedSyncGossip);
            logger.info("receivedDupesSyncGossip: {}", PlumtreeStats.receivedDupesSyncGossip);
        }

        //Flood
        else if(broadcastId == FloodBroadcast.PROTOCOL_ID) {
            logger.info("sentFlood: {}", FloodStats.sentFlood);
            logger.info("sentSendVC: {}", FloodStats.sentSendVC);
            logger.info("sentVC: {}", FloodStats.sentVC);
            logger.info("sentSyncOps: {}", FloodStats.sentSyncOps);
            logger.info("sentSyncFlood: {}", FloodStats.sentSyncFlood);

            logger.info("receivedFlood: {}", FloodStats.receivedFlood);
            logger.info("receivedDupesFlood: {}", FloodStats.receivedDupesFlood);
            logger.info("receivedSendVC: {}", FloodStats.receivedSendVC);
            logger.info("receivedVC: {}", FloodStats.receivedVC);
            logger.info("receivedSyncOps: {}", FloodStats.receivedSyncOps);
            logger.info("receivedSyncFlood: {}", FloodStats.receivedSyncFlood);
            logger.info("receivedDupesSyncFlood: {}", FloodStats.receivedDupesSyncFlood);
        }

        //Periodic Pull
        else if(broadcastId == PeriodicPullBroadcast.PROTOCOL_ID) {
            logger.info("sentVC: {}", PeriodicPullBroadcast.sentVC);
            logger.info("sentSyncOps: {}", PeriodicPullBroadcast.sentSyncOps);
            logger.info("sentSyncPull: {}", PeriodicPullBroadcast.sentSyncPull);

            logger.info("receivedVC: {}", PeriodicPullBroadcast.receivedVC);
            logger.info("receivedSyncOps: {}", PeriodicPullBroadcast.receivedSyncOps);
            logger.info("receivedSyncPull: {}", PeriodicPullBroadcast.receivedSyncPull);
            logger.info("receivedDupes: {}", PeriodicPullBroadcast.receivedDupes);
        }

    }

    private int getCounterValue(String crdtId) {
        return ((CounterCRDT) myCRDTs.get(crdtId)).value();
    }

    private Object getRegisterValue(String crdtId) {
        return ((RegisterCRDT) myCRDTs.get(crdtId)).value();
    }

    private List<SerializableType> getSetValue(String crdtId) {
        List<SerializableType> list = new LinkedList<>(((SetCRDT) myCRDTs.get(crdtId)).elements());
        Collections.sort(list);
        return list;
    }

    private Set<SerializableType> getMapKeys(String crdtId) {
        return ((MapCRDT) myCRDTs.get(crdtId)).keys();
    }

    private List<SerializableType> getMapValues(String crdtId) {
        List<SerializableType> list = new LinkedList<>(((MapCRDT) myCRDTs.get(crdtId)).values());
        Collections.sort(list);
        return list;
    }

    private List<SerializableType> getMapping(String crdtId, SerializableType key) {
        List<SerializableType> list = new LinkedList<>(((MapCRDT) myCRDTs.get(crdtId)).get(key));
        Collections.sort(list);
        return list;
    }


    /* --------------------------------- Unused Methods --------------------------------- */

    private void executeOp1(int run) {
        if(run == 0) {
            executeCounterOperation(CRDT0, CounterOpType.INCREMENT, 1);
        } else if(run == 1) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
        } else if(run == 2) {
            executeSetOperation(CRDT2, SetOpType.ADD, new IntegerType(rand.nextInt(10)));
        } else if(run == 3) {
            executeMapOperation(CRDT3, MapOpType.PUT, new ByteType((byte)rand.nextInt(2)), new IntegerType(rand.nextInt(10)));
        } else if(run == 4) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(5));
            executeRegisterOperation(CRDT2, RegisterOpType.ASSIGN, new LongType(5L));
            executeRegisterOperation(CRDT3, RegisterOpType.ASSIGN, new ShortType((short)2));
            executeRegisterOperation(CRDT4, RegisterOpType.ASSIGN, new FloatType(8f));
            executeRegisterOperation(CRDT5, RegisterOpType.ASSIGN, new DoubleType(1.4));
            executeRegisterOperation(CRDT6, RegisterOpType.ASSIGN, new StringType("Olá"));
            executeRegisterOperation(CRDT7, RegisterOpType.ASSIGN, new BooleanType(true));
            executeRegisterOperation(CRDT8, RegisterOpType.ASSIGN, new ByteType((byte)0));
        } else if(run == 5) {
            executeSetOperation(CRDT1, SetOpType.ADD, new IntegerType(1));
            executeSetOperation(CRDT2, SetOpType.ADD, new LongType(5L));
            executeSetOperation(CRDT3, SetOpType.ADD, new ShortType((short)2));
            executeSetOperation(CRDT4, SetOpType.ADD, new FloatType(8f));
            executeSetOperation(CRDT5, SetOpType.ADD, new DoubleType(1.4));
            executeSetOperation(CRDT6, SetOpType.ADD, new StringType("Olá"));
            executeSetOperation(CRDT7, SetOpType.ADD, new BooleanType(true));
            executeSetOperation(CRDT8, SetOpType.ADD, new ByteType((byte)0));
        } else if(run == 6) {
            executeMapOperation(CRDT1, MapOpType.PUT, new ByteType((byte)1), new IntegerType(1));
            executeMapOperation(CRDT2, MapOpType.PUT, new ByteType((byte)1), new ShortType((short)4));
            executeMapOperation(CRDT3, MapOpType.PUT, new ByteType((byte)1), new LongType(5L));
            executeMapOperation(CRDT4, MapOpType.PUT, new ByteType((byte)1), new FloatType(8f));
            executeMapOperation(CRDT5, MapOpType.PUT, new ByteType((byte)1), new DoubleType(1.4));
            executeMapOperation(CRDT6, MapOpType.PUT, new ByteType((byte)1), new BooleanType(true));
            executeMapOperation(CRDT7, MapOpType.PUT, new ByteType((byte)1), new StringType("Olá, bom dia"));
            executeMapOperation(CRDT8, MapOpType.PUT, new ByteType((byte)1), new ByteType((byte)0));
        } else if(run == 7) {
            executeCounterOperation(CRDT0, CounterOpType.INCREMENT, 1);
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
            executeSetOperation(CRDT2, SetOpType.ADD, new IntegerType(rand.nextInt(10)));
            executeMapOperation(CRDT3, MapOpType.PUT, new ByteType((byte)rand.nextInt(2)), new IntegerType(rand.nextInt(10)));
        }
    }

    private void executeOp2(int run) {
        if(run == 0) {
            executeCounterOperation(CRDT0, CounterOpType.DECREMENT, 1);
        } else if(run == 1) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
        } else if(run == 2) {
            executeSetOperation(CRDT2, SetOpType.REMOVE, new IntegerType(rand.nextInt(10)));
        } else if(run == 3) {
            executeMapOperation(CRDT3, MapOpType.DELETE, new ByteType((byte)rand.nextInt(2)), null);
        } else if(run == 4) {
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(7));
            executeRegisterOperation(CRDT2, RegisterOpType.ASSIGN, new LongType(8L));
            executeRegisterOperation(CRDT3, RegisterOpType.ASSIGN, new ShortType((short)4));
            executeRegisterOperation(CRDT4, RegisterOpType.ASSIGN, new FloatType(9f));
            executeRegisterOperation(CRDT5, RegisterOpType.ASSIGN, new DoubleType(1.35));
            executeRegisterOperation(CRDT6, RegisterOpType.ASSIGN, new StringType("Bom dia"));
            executeRegisterOperation(CRDT7, RegisterOpType.ASSIGN, new BooleanType(false));
            executeRegisterOperation(CRDT8, RegisterOpType.ASSIGN, new ByteType((byte)1));
        } else if(run == 5) {
            executeSetOperation(CRDT1, SetOpType.REMOVE, new IntegerType(1));
            executeSetOperation(CRDT2, SetOpType.REMOVE, new LongType(5L));
            executeSetOperation(CRDT3, SetOpType.REMOVE, new ShortType((short)2));
            executeSetOperation(CRDT4, SetOpType.REMOVE, new FloatType(8f));
            executeSetOperation(CRDT5, SetOpType.REMOVE, new DoubleType(1.4));
            executeSetOperation(CRDT6, SetOpType.REMOVE, new StringType("Olá"));
            executeSetOperation(CRDT7, SetOpType.REMOVE, new BooleanType(true));
            executeSetOperation(CRDT8, SetOpType.REMOVE, new ByteType((byte)0));
        } else if(run == 6) {
            executeMapOperation(CRDT1, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT2, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT3, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT4, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT5, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT6, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT7, MapOpType.DELETE, new ByteType((byte)1), null);
            executeMapOperation(CRDT8, MapOpType.DELETE, new ByteType((byte)1), null);
        } else if(run == 7) {
            executeCounterOperation(CRDT0, CounterOpType.DECREMENT, 1);
            executeRegisterOperation(CRDT1, RegisterOpType.ASSIGN, new IntegerType(rand.nextInt(10)));
            executeSetOperation(CRDT2, SetOpType.REMOVE, new IntegerType(rand.nextInt(10)));
            executeMapOperation(CRDT3, MapOpType.DELETE, new ByteType((byte)rand.nextInt(2)), null);
        }
    }
}
