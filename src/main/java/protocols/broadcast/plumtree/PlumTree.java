package protocols.broadcast.plumtree;

import crdts.utils.VectorClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.utils.MyFileManager;
import protocols.broadcast.common.messages.SendVectorClockMessage;
import protocols.broadcast.common.messages.SyncOpsMessage;
import protocols.broadcast.common.messages.VectorClockMessage;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.plumtree.messages.*;
import protocols.broadcast.common.timers.ReconnectTimeout;
import protocols.broadcast.plumtree.timers.IHaveTimeout;
import protocols.broadcast.plumtree.timers.SendTreeMessageTimeout;
import protocols.broadcast.plumtree.utils.*;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.*;
import java.net.InetAddress;
import java.util.*;

public class PlumTree extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PlumTree.class);

    public final static short PROTOCOL_ID = 900;
    public final static String PROTOCOL_NAME = "BCAST-PlumTree";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;

    private final long timeout1;
    private final long reconnectTimeout;
    private final long treeMsgTimeout;
    private final long treeMsgStartTime;
    private final boolean iAmTreeMsgSender;
    private final boolean startInLazy;

    private final Set<Host> partialView;
    private final Map<Host, VectorClock> eager;
    private final Set<Host> lazy;

    private final Set<OutgoingSync> outgoingSyncs; // Hosts we have asked for vc
    private IncomingSync incomingSync; // Host that we sent our vc to
    private final Queue<IncomingSync> pendingIncomingSyncs; // Queue of pending incoming syncs

    private final Map<UUID, Queue<Host>> missing; // Queue of hosts that have announced having a msg ID we do not have
    private final Set<UUID> received; // IDs of received gossip msgs
    private final Set<UUID> receivedTreeIDs; // IDs of received tree msgs
    private final Map<UUID, Long> onGoingTimers; // Timers for tree msgs reception

    public static VectorClock vectorClock; // Local vector clock
    private int seqNumber; // Counter of local operations

    private final MyFileManager fileManager;

    /***** Stats *****/ //TODO: classe de stats
    public static int sentTree;
    public static int sentGossip;
    public static int sentIHave;
    public static int sentGraft;
    public static int sentPrune;
    public static int sentSendVC;
    public static int sentVC;
    public static int sentSyncOps;
    public static int sentSyncGossip;

    public static int receivedTree;
    public static int receivedGossip;
    public static int receivedDupesTree;
    public static int receivedDupesGossip;
    public static int receivedIHave;
    public static int receivedGraft;
    public static int receivedPrune;
    public static int receivedSendVC;
    public static int receivedVC;
    public static int receivedSyncOps;
    public static int receivedSyncGossip;
    public static int receivedDupesSyncGossip;


    /*--------------------------------- Initialization ---------------------------------------- */

    public PlumTree(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.timeout1 = Long.parseLong(properties.getProperty("timeout1", "1000"));
        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));
        this.treeMsgTimeout = Long.parseLong(properties.getProperty("tree_msg_timeout", "1000"));
        this.treeMsgStartTime = Long.parseLong(properties.getProperty("tree_msg_start", "60000"));
        this.iAmTreeMsgSender = myself.equals(new Host(InetAddress.getByName("10.10.0.10"),6000));
        this.startInLazy = properties.getProperty("start_in_lazy", "false").equals("true");

        this.partialView = new HashSet<>();
        this.eager = new HashMap<>();
        this.lazy = new HashSet<>();

        this.outgoingSyncs = new HashSet<>();
        this.incomingSync = new IncomingSync(null, null);
        this.pendingIncomingSyncs = new LinkedList<>();

        this.missing = new HashMap<>();
        this.received = new HashSet<>();
        this.receivedTreeIDs = new HashSet<>();
        this.onGoingTimers = new HashMap<>();

        vectorClock = new VectorClock(myself);

        this.fileManager = new MyFileManager(properties, myself);

        String cMetricsInterval = properties.getProperty("bcast_channel_metrics_interval", "10000"); // 10 seconds

        // Create a properties object to set up channel-specific properties. See the
        // channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, properties.getProperty("address")); // The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, properties.getProperty("bcast_port")); // The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel
        // metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established
        // connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until
        // closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(SendTreeMessageTimeout.TIMER_ID, this::uponSendTreeMessageTimeout);
        registerTimerHandler(IHaveTimeout.TIMER_ID, this::uponIHaveTimeout);
        registerTimerHandler(ReconnectTimeout.TIMER_ID, this::uponReconnectTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, TreeMessage.MSG_ID, TreeMessage.serializer);
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_ID, GraftMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_ID, IHaveMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SyncOpsMessage.MSG_ID, SyncOpsMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, TreeMessage.MSG_ID, this::uponReceiveTreeMessage, this::onMessageFailed);
        registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossip, this::onMessageFailed);
        registerMessageHandler(channelId, PruneMessage.MSG_ID, this::uponReceivePrune, this::onMessageFailed);
        registerMessageHandler(channelId, GraftMessage.MSG_ID, this::uponReceiveGraft, this::onMessageFailed);
        registerMessageHandler(channelId, IHaveMessage.MSG_ID, this::uponReceiveIHave, this::onMessageFailed);

        registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponReceiveSendVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClock, this::onMessageFailed);
        registerMessageHandler(channelId, SyncOpsMessage.MSG_ID, this::uponReceiveSyncOps, this::onMessageFailed);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        setupPeriodicTimer(new SendTreeMessageTimeout(), treeMsgStartTime, treeMsgTimeout);
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        receivedGossip++;
        UUID mid = request.getMsgId();
        byte[] content = request.getMsg();
        logger.info("SENT {}", mid);
        logger.info("RECEIVED {}", mid);
        triggerNotification(new DeliverNotification(mid, myself, content, false));
        GossipMessage msg = new GossipMessage(mid, myself, ++seqNumber, content);
        logger.debug("Accepted my op {}-{}: {} to {}", myself, seqNumber, mid, eager);
        try {
            this.fileManager.writeOperationToFile(myself, seqNumber, msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        handleGossipMessage(msg, myself, false);
    }

    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveTreeMessage(TreeMessage msg, Host from, short sourceProto, int channelId) {
        receivedTree++;
        UUID mid = msg.getMid();
        logger.debug("Received tree {} from {}", mid, from);
        if (!receivedTreeIDs.contains(mid)) {
            handleTreeMessage(msg, from);
        } else {
            receivedDupesTree++;
            logger.info("dupe tree from {}", from);
            logger.debug("{} was duplicated tree from {}", mid, from);
            StringBuilder sb = new StringBuilder("VIS-TREEDUPE: ");
            boolean print = false;

            if(partialView.contains(from)) { //Because we can receive messages before neigh up
                if (eager.remove(from) != null) {
                    logger.debug("Removed {} from eager due to duplicate tree {}", from, eager);
                    print = true;
                    sb.append(String.format("Removed %s from eager; ", from));
                }

                if (outgoingSyncs.remove(new OutgoingSync(from))) {
                    logger.debug("Removed {} from outgoingSyncs due to duplicate", from);
                    print = true;
                    sb.append(String.format("Removed %s from outgoingSyncs; ", from));
                }

                if (lazy.add(from)) {
                    logger.debug("Added {} to lazy due to duplicate {}", from, lazy);
                    print = true;
                    sb.append(String.format("Added %s to lazy; ", from));
                }

                logger.debug("Sent PruneMessage to {}", from);
                sendMessage(new PruneMessage(), from);
                sentPrune++;
            }

            if(print) {
                sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
                logger.info(sb);
            }
        }
    }

    private void uponReceiveGossip(GossipMessage msg, Host from, short sourceProto, int channelId) {
        receivedGossip++;
        UUID mid = msg.getMid();
        logger.debug("Received gossip {} from {}", mid, from);
        if (!received.contains(mid)) {
            logger.info("RECEIVED {}", mid);
            Host h = msg.getOriginalSender();
            int clock = msg.getSenderClock();
            if (vectorClock.getHostClock(h) == clock - 1) {
                logger.debug("[{}] Accepted op {}-{} : {} from {}, Clock {}", false,
                        h, clock, mid, from, vectorClock.getHostClock(h));
                triggerNotification(new DeliverNotification(mid, from, msg.getContent(), false));
                handleGossipMessage(msg, from, false);
            } else if (vectorClock.getHostClock(h) < clock - 1) {
                logger.error("[{}] Out-of-order op {}-{} : {} from {}, Clock {}", false,
                        h, clock, mid, from, vectorClock.getHostClock(h));
            } else {
                logger.error("[{}] Ignored old op {}-{} : {} from {}, Clock {}", false,
                        h, clock, mid, from, vectorClock.getHostClock(h));
            }
        } else {
            receivedDupesGossip++;
            logger.info("DUPLICATE GOSSIP from {}", from);
        }
    }

    private void uponReceivePrune(PruneMessage msg, Host from, short sourceProto, int channelId) {
        receivedPrune++;

        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder("VIS-PRUNE: ");

        if (eager.remove(from) != null) {
            logger.debug("Removed {} from eager due to prune {}", from, eager);
            sb.append(String.format("Removed %s from eager; ", from));

            if (lazy.add(from)) {
                logger.debug("Added {} to lazy due to prune {}", from, lazy);
                sb.append(String.format("Added %s to lazy; ", from));
            }

            if (from.equals(incomingSync.getHost())) {
                logger.debug("Removed {} from incomingSync due to prune", from);
                sb.append(String.format("Removed %s from incomingSync; ", from));
                tryNextIncomingSync();
            }

            if (removeFromPendingIncomingSyncs(from)) {
                logger.debug("Removed {} from pendingIncomingSyncs due to prune {}", from, pendingIncomingSyncs);
                sb.append(String.format("Removed %s from pendingIncomingSyncs; ", from));
            }

            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }
    }

    private void uponReceiveGraft(GraftMessage msg, Host from, short sourceProto, int channelId) {
        receivedGraft++;

        logger.debug("Received {} from {}", msg, from);
        startSynchronization(from, false, UUID.randomUUID(), "GRAFT");
    }

    private void uponReceiveIHave(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        receivedIHave++;

        logger.debug("Received {} from {}", msg, from);
        handleAnnouncement(msg.getMid(), from);
    }

    private void uponReceiveVectorClock(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        receivedVC++;
        logger.debug("Received {} from {}", msg, from);

        if(outgoingSyncs.contains(new OutgoingSync(from))) { // If sync was not cancelled
            SyncOpsMessage syncOpsMessages = this.fileManager.readSyncOpsFromFile(msg.getMid(), msg.getVectorClock(), vectorClock);
            sendMessage(syncOpsMessages, from);
            sentSyncOps++;
            sentSyncGossip += syncOpsMessages.getMsgs().size();
            logger.debug("Sent {} to {}", syncOpsMessages, from);
            addNeighbourToEager(from, msg.getVectorClock());
        }
    }

    private void uponReceiveSendVectorClock(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        receivedSendVC++;

        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder("VIS-SENDVC: ");

        UUID mid = msg.getMid();
        Host currentPending = incomingSync.getHost();

        if(currentPending == null) {
            incomingSync = new IncomingSync(from, mid);
            logger.debug("{} is my incomingSync ", from);
            sb.append(String.format("Added %s to incomingSync; ", from));
            VectorClockMessage vectorClockMessage = new VectorClockMessage(mid, myself, new VectorClock(vectorClock.getClock()));
            sendMessage(vectorClockMessage, from, TCPChannel.CONNECTION_IN);
            sentVC++;
            logger.debug("Sent {} to {}", vectorClockMessage, from);
        } else {
            pendingIncomingSyncs.add(new IncomingSync(from, mid));
            logger.debug("Added {} to pendingIncomingSyncs {}", from, pendingIncomingSyncs);
            sb.append(String.format("Added %s to pendingIncomingSyncs; ", from));
        }

        sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
        logger.info(sb);

    }

    private void uponReceiveSyncOps(SyncOpsMessage msg, Host from, short sourceProto, int channelId) {
        receivedSyncOps++;

        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder("VIS-SYNCOPS: ");

        for (byte[] serMsg : msg.getMsgs()) {
            receivedSyncGossip++;

            GossipMessage gossipMessage = null;
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serMsg));
            try {
                gossipMessage = GossipMessage.deserialize(dis);
            } catch (IOException e) {
                e.printStackTrace();
            }

            UUID mid = gossipMessage.getMid();

            if (!received.contains(mid)) {
                logger.info("RECEIVED {}", mid);
                Host h = gossipMessage.getOriginalSender();
                int clock = gossipMessage.getSenderClock();
                if (vectorClock.getHostClock(h) == clock - 1) {
                    logger.debug("[{}] Accepted op {}-{} : {} from {}, Clock {}", true,
                            h, clock, mid, from, vectorClock.getHostClock(h));
                    triggerNotification(new DeliverNotification(mid, from, gossipMessage.getContent(), true));
                    logger.debug("Propagating sync op {} to {}", mid, eager);
                    handleGossipMessage(gossipMessage, from, true);
                } else if (vectorClock.getHostClock(h) < clock - 1) {
                    logger.error("[{}] Out-of-order op {}-{} : {} from {}, Clock {}", true,
                            h, clock, mid, from, vectorClock.getHostClock(h));
                } else {
                    logger.error("[{}] Ignored old op {}-{} : {} from {}, Clock {}", true,
                            h, clock, mid, from, vectorClock.getHostClock(h));
                }
            } else {
                logger.info("DUPLICATE SYNC from {}", from);
                logger.debug("Sync op {} was dupe", mid);
                receivedDupesSyncGossip++;
            }
        }
        sb.append(String.format("Removed %s from incomingSync; ", from));
        sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
        logger.info(sb);
        logger.info("Sync {} ENDED", msg.getMid());
        tryNextIncomingSync();
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponSendTreeMessageTimeout(SendTreeMessageTimeout timeout, long timerId) {
        if(iAmTreeMsgSender) {
            UUID mid = UUID.randomUUID();
            logger.debug("Generated tree msg {}", mid);
            TreeMessage msg = new TreeMessage(mid, myself);
            handleTreeMessage(msg, myself);
        }
    }

    private void uponIHaveTimeout(IHaveTimeout timeout, long timerId) {
        UUID mid = timeout.getMid();
        if (!receivedTreeIDs.contains(mid)) {
            Host msgSrc = missing.get(mid).poll();
            if (msgSrc != null) {
                logger.debug("Try sync with {} for timeout {}", msgSrc, mid);
                startSynchronization(msgSrc, false, mid, "TIMEOUT-" + mid);
            }
        }
    }

    private void uponReconnectTimeout(ReconnectTimeout timeout, long timerId) {
        Host neighbour = timeout.getHost();
        if (partialView.contains(neighbour)) {
            logger.debug("Reconnecting with {}", neighbour);
            openConnection(neighbour);
        } else {
            logger.debug("Not reconnecting because {} is down", neighbour);
        }
    }


    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        if (partialView.add(neighbour)) {
            logger.debug("Added {} to partial view due to up {}", neighbour, partialView);
        } else {
            logger.error("Tried to add {} to partial view but is already there {}", neighbour, partialView);
        }

        openConnection(neighbour);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        StringBuilder sb = new StringBuilder("VIS-NEIGHDOWN: ");
        boolean print = false;

        if (partialView.remove(neighbour)) {
            logger.debug("Removed {} from partial view due to down {}", neighbour, partialView);
        }

        if (eager.remove(neighbour) != null) {
            logger.debug("Removed {} from eager due to down {}", neighbour, eager);
            print = true;
            sb.append(String.format("Removed %s from eager; ", neighbour));
        }

        if (lazy.remove(neighbour)) {
            logger.debug("Removed {} from lazy due to down {}", neighbour, lazy);
            print = true;
            sb.append(String.format("Removed %s from lazy; ", neighbour));
        }

        if (removeFromPendingIncomingSyncs(neighbour)) {
            logger.debug("Removed {} from pendingIncomingSyncs due to down {}", neighbour, pendingIncomingSyncs);
            print = true;
            sb.append(String.format("Removed %s from pendingIncomingSyncs; ", neighbour));
        }

        if (outgoingSyncs.remove(new OutgoingSync(neighbour))) {
            logger.debug("Removed {} from outgoingSyncs due to down {}", neighbour, outgoingSyncs);
            print = true;
            sb.append(String.format("Removed %s from outgoingSyncs; ", neighbour));
        }

        for (Queue<Host> iHaves : missing.values()) {
            iHaves.remove(neighbour);
        }

        if (neighbour.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from incomingSync due to down", neighbour);
            print = true;
            sb.append(String.format("Removed %s from incomingSync; ", neighbour));
            tryNextIncomingSync();
        }

        if(print) {
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }

        closeConnection(neighbour);
    }


    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.trace("Host {} is down, cause: {}", host, event.getCause());

        StringBuilder sb = new StringBuilder("VIS-CONNDOWN: ");
        boolean print = false;

        if (eager.remove(host) != null) {
            logger.debug("Removed {} from eager due to plumtree down {}", host, eager);
            print = true;
            sb.append(String.format("Removed %s from eager; ", host));
        }

        if (lazy.remove(host)) {
            logger.debug("Removed {} from lazy due to plumtree down {}", host, lazy);
            print = true;
            sb.append(String.format("Removed %s from lazy; ", host));
        }

        if (removeFromPendingIncomingSyncs(host)) {
            logger.debug("Removed {} from pendingIncomingSyncs due to plumtree down {}", host, pendingIncomingSyncs);
            print = true;
            sb.append(String.format("Removed %s from pendingIncomingSyncs; ", host));
        }

        if (outgoingSyncs.remove(new OutgoingSync(host))) {
            logger.debug("Removed {} from outgoingSyncs due to plumtree down {}", host, outgoingSyncs);
            print = true;
            sb.append(String.format("Removed %s from outgoingSyncs; ", host));
        }

        if (host.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from incomingSync due to plumtree down", host);
            print = true;
            sb.append(String.format("Removed %s from incomingSync; ", host));
            tryNextIncomingSync();
        }

        if(print) {
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }

        if(partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

    @SuppressWarnings("rawtypes")
    private void uponOutConnectionFailed(OutConnectionFailed event, int channelId) {
        Host host = event.getNode();
        logger.trace("Connection to host {} failed, cause: {}", host, event.getCause());
        if(partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host neighbour = event.getNode();
        logger.trace("Host (out) {} is up", neighbour);

        StringBuilder sb = new StringBuilder("VIS-CONNUP: ");

        if (partialView.contains(neighbour)) {
            if (startInLazy) {
                if (lazy.add(neighbour)) {
                    logger.debug("Added {} to lazy due to neigh up {}", neighbour, lazy);
                    sb.append(String.format("Added %s to lazy; ", neighbour));
                    sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
                    logger.info(sb);
                }
            } else {
                logger.debug("Trying sync from neighbour {} up", neighbour);
                startSynchronization(neighbour, true, UUID.randomUUID(), "NEIGHUP");
            }
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.trace("Connection from host {} is down, cause: {}", host, event.getCause());

        StringBuilder sb = new StringBuilder("VIS-INCONNDOWN: ");
        boolean print = false;

        if (host.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from incomingSync due to in connection down", host);
            print = true;
            sb.append(String.format("Removed %s from incomingSync; ", host));
            tryNextIncomingSync();
        }

        if (removeFromPendingIncomingSyncs(host)) {
            logger.debug("Removed {} from pendingIncomingSyncs due to in connection down {}", host, pendingIncomingSyncs);
            print = true;
            sb.append(String.format("Removed %s from pendingIncomingSyncs; ", host));
        }

        if(print)
            logger.info(sb);
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void startSynchronization(Host neighbour, boolean neighUp, UUID msgId, String cause) {
        StringBuilder sb = new StringBuilder("VIS-STARTSYNC-" + cause + ": ");
        OutgoingSync os = new OutgoingSync(neighbour, neighUp, msgId, cause);

        if (partialView.contains(neighbour) && !outgoingSyncs.contains(os) && !eager.containsKey(neighbour)) {
            logger.debug("Sent GraftMessage for {} to {}", msgId, neighbour);
            sendMessage(new GraftMessage(msgId), neighbour);
            sentGraft++;
        }

        if (neighUp || (lazy.contains(neighbour) && !outgoingSyncs.contains(os))) {
            logger.debug("Added {} to outgoingSyncs", neighbour);
            outgoingSyncs.add(os);
            UUID mid = UUID.randomUUID();
            SendVectorClockMessage msg = new SendVectorClockMessage(mid);
            sendMessage(msg, neighbour);
            logger.info("Sync {} STARTED", mid);
            sentSendVC++;
            logger.debug("Sent {} to {}", msg, neighbour);
            sb.append(String.format("Added %s to outgoingSyncs; ", neighbour));
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }
    }

    private void addNeighbourToEager(Host neighbour, VectorClock vectorClock) {

        if(partialView.contains(neighbour)) {
            StringBuilder sb = new StringBuilder("VIS-ENDSYNC: ");

            if (eager.put(neighbour, vectorClock) == null) {
                logger.debug("Added {} to eager {} : pendingIncomingSyncs {}", neighbour, eager, pendingIncomingSyncs);
                sb.append(String.format("Added %s to eager; ", neighbour));
            }

            if (outgoingSyncs.remove(new OutgoingSync(neighbour))) {
                logger.debug("Removed {} from outgoingSyncs due to sync {}", neighbour, outgoingSyncs);
                sb.append(String.format("Removed %s from outgoingSyncs; ", neighbour));
            }

            if (lazy.remove(neighbour)) {
                logger.debug("Removed {} from lazy due to sync {}", neighbour, lazy);
                sb.append(String.format("Removed %s from lazy; ", neighbour));
            }

            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }
    }

    private void tryNextIncomingSync() {
        StringBuilder sb = new StringBuilder("VIS-NEXTINCOMINGSYNC: ");
        IncomingSync nextIncomingSync = pendingIncomingSyncs.poll();

        if (nextIncomingSync != null) {
            Host currentPending = nextIncomingSync.getHost();
            UUID mid = nextIncomingSync.getMid();
            incomingSync = nextIncomingSync;
            logger.debug("{} is my incomingSync try", currentPending);
            sb.append(String.format("Removed %s from pendingIncomingSyncs; ", currentPending));
            sb.append(String.format("Added %s to incomingSync; ", currentPending));
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager, lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
            VectorClockMessage vectorClockMessage = new VectorClockMessage(mid, myself, new VectorClock(vectorClock.getClock()));
            sendMessage(vectorClockMessage, currentPending, TCPChannel.CONNECTION_IN);
            sentVC++;
            logger.debug("Sent {} to {}", vectorClockMessage, currentPending);
        } else {
            incomingSync = new IncomingSync(null, null);
        }
    }

    private void handleTreeMessage(TreeMessage msg, Host from) {
        UUID mid = msg.getMid();
        receivedTreeIDs.add(mid);

        Long tid;
        if ((tid = onGoingTimers.remove(mid)) != null) {
            cancelTimer(tid);
        }

        for (Host peer : eager.keySet()) {
            if (!peer.equals(from)) {
                sendMessage(msg, peer);
                sentTree++;
                logger.debug("Forward tree {} received from {} to {}", msg.getMid(), from, peer);
            }
        }

        for (Host peer : lazy) {
            if (!peer.equals(from)) {
                IHaveMessage iHave = new IHaveMessage(msg.getMid());
                logger.debug("Sent {} to {}", iHave, peer);
                sendMessage(iHave, peer);
                sentIHave++;
            }
        }
    }

    private void handleGossipMessage(GossipMessage msg, Host from, boolean isFromSync) {
        vectorClock.incrementClock(msg.getOriginalSender());

        UUID mid = msg.getMid();
        received.add(mid);
        for (Map.Entry<Host, VectorClock> entry : eager.entrySet()) {
            Host peer = entry.getKey();
            if (!peer.equals(from)) {
                VectorClock vc = entry.getValue();
                if (!isFromSync || vc.getHostClock(msg.getOriginalSender()) < msg.getSenderClock()) {
                    sendMessage(msg, peer);
                    sentGossip++;
                    logger.debug("Forward gossip {} received from {} to {}", msg.getMid(), from, peer);
                }
            }
        }
    }

    private void handleAnnouncement(UUID mid, Host from) {
        if (!receivedTreeIDs.contains(mid)) {
            if (eager.isEmpty() && outgoingSyncs.isEmpty()) {
                logger.debug("Try sync with {} before timeout {}", from, mid);
                startSynchronization(from, false, mid, "BEFORETIMEOUT-" + mid);
            } else {
                if (!onGoingTimers.containsKey(mid)) {
                    long tid = setupTimer(new IHaveTimeout(mid), timeout1);
                    onGoingTimers.put(mid, tid);
                }
                missing.computeIfAbsent(mid, v -> new LinkedList<>()).add(from);
            }
        }
    }

    private boolean removeFromPendingIncomingSyncs(Host host) {
        boolean removed = false;
        Iterator<IncomingSync> it = this.pendingIncomingSyncs.iterator();
        while(it.hasNext()) {
            if(it.next().getHost().equals(host)) {
                removed = true;
                it.remove();
            }
        }
        return removed;
    }

    /*--------------------------------- Metrics ---------------------------------*/

    /**
     * If we passed a value > 0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
     * periodically by the channel. "getInConnections" and "getOutConnections" returns the currently established
     * connection to/from me. "getOldInConnections" and "getOldOutConnections" returns connections that have already
     * been closed.
     */
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics: ");
        long bytesSent = 0;
        long bytesReceived = 0;

        for(ChannelMetrics.ConnectionMetrics c: event.getOutConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getOldOutConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getInConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        for(ChannelMetrics.ConnectionMetrics c: event.getOldInConnections()){
            bytesSent += c.getSentAppBytes();
            bytesReceived += c.getReceivedAppBytes();
        }

        sb.append(String.format("BytesSent=%s ", bytesSent));
        sb.append(String.format("BytesReceived=%s", bytesReceived));
        logger.info(sb);
    }

}