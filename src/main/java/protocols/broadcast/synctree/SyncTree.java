package protocols.broadcast.synctree;

import protocols.broadcast.common.messages.GossipMessage;
import protocols.broadcast.common.notifications.InstallStateNotification;
import protocols.broadcast.common.requests.UpdateStateRequest;
import protocols.broadcast.common.utils.CommunicationCostCalculator;
import protocols.broadcast.common.utils.MultiFileManager;
import protocols.broadcast.common.utils.StateAndVC;
import protocols.broadcast.common.utils.VectorClock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.SendVectorClockMessage;
import protocols.broadcast.common.messages.SynchronizationMessage;
import protocols.broadcast.common.messages.VectorClockMessage;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.synctree.messages.*;
import protocols.broadcast.common.timers.ReconnectTimeout;
import protocols.broadcast.synctree.timers.*;
import protocols.broadcast.synctree.utils.*;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.*;
import java.util.*;

public class SyncTree extends CommunicationCostCalculator {

    private static final Logger logger = LogManager.getLogger(SyncTree.class);

    public final static short PROTOCOL_ID = 900;
    public final static String PROTOCOL_NAME = "BCAST-SyncTree";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;
    private final static int SECONDS_TO_MILLIS = 1000;

    private final long reconnectTimeout;
    private final long diskUsageTimeout;

    private final long iHaveTimeout; // Timeout to send announcements
    private final long treeMsgTimeout; // Timeout to send tree message
    private final long checkTreeMsgsTimeout; // Timeout to check if tree messages have been received recently

    private final long garbageCollectionTimeout; // Timeout to garbage collect old operations
    private StateAndVC stateAndVC; // Current state and corresponding VC

    private long sendTreeMsgTimer; // Timer to send tree messages
    private int treeMsgsFromSmallerHost; // Number of tree messages received from relevant
    // peers since the last timer expired

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

    private final MultiFileManager fileManager;

    private final PlumtreeStats stats;


    /*--------------------------------- Initialization ---------------------------------------- */

    public SyncTree(Properties properties, Host myself) throws HandlerRegistrationException, IOException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;

        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));
        this.diskUsageTimeout = Long.parseLong(properties.getProperty("disk_usage_timeout", "1000"));

        this.iHaveTimeout = Long.parseLong(properties.getProperty("i_have_timeout", "1000"));
        this.treeMsgTimeout = Long.parseLong(properties.getProperty("tree_msg_timeout", "100"));
        this.checkTreeMsgsTimeout = Long.parseLong(properties.getProperty("check_tree_msgs_timeout", "5000"));

        this.garbageCollectionTimeout = Long.parseLong(properties.getProperty("garbage_collection_timeout", "15")) * SECONDS_TO_MILLIS;
        long garbageCollectionTTL = Long.parseLong(properties.getProperty("garbage_collection_ttl", "60")) * SECONDS_TO_MILLIS;
        this.stateAndVC = new StateAndVC(null, new VectorClock(myself));

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

        int indexSpacing = Integer.parseInt(properties.getProperty("index_spacing", "100"));
        this.fileManager = new MultiFileManager(garbageCollectionTimeout, garbageCollectionTTL, indexSpacing, myself);

        this.stats = new PlumtreeStats();

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
        registerTimerHandler(CheckReceivedTreeMessagesTimeout.TIMER_ID, this::uponCheckReceivedTreeMessagesTimeout);
        registerTimerHandler(IHaveTimeout.TIMER_ID, this::uponIHaveTimeout);
        registerTimerHandler(ReconnectTimeout.TIMER_ID, this::uponReconnectTimeout);
        registerTimerHandler(PrintDiskUsageTimeout.TIMER_ID, this::uponPrintDiskUsageTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        registerRequestHandler(UpdateStateRequest.REQUEST_ID, this::uponUpdateStateRequest);

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
        registerMessageSerializer(channelId, SynchronizationMessage.MSG_ID, SynchronizationMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, TreeMessage.MSG_ID, this::uponReceiveTreeMsg, this::onMessageFailed);
        registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponReceiveGossipMsg, this::onMessageFailed);
        registerMessageHandler(channelId, PruneMessage.MSG_ID, this::uponReceivePruneMsg, this::onMessageFailed);
        registerMessageHandler(channelId, GraftMessage.MSG_ID, this::uponReceiveGraftMsg, this::onMessageFailed);
        registerMessageHandler(channelId, IHaveMessage.MSG_ID, this::uponReceiveIHaveMsg, this::onMessageFailed);

        registerMessageHandler(channelId, SendVectorClockMessage.MSG_ID, this::uponReceiveSendVectorClockMsg, this::onMessageFailed);
        registerMessageHandler(channelId, VectorClockMessage.MSG_ID, this::uponReceiveVectorClockMsg, this::onMessageFailed);
        registerMessageHandler(channelId, SynchronizationMessage.MSG_ID, this::uponReceiveSynchronizationMsg, this::onMessageFailed);

        /*-------------------- Register Channel Event ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }

    @Override
    public void init(Properties props) {
        setupPeriodicTimer(new CheckReceivedTreeMessagesTimeout(), checkTreeMsgsTimeout, checkTreeMsgsTimeout);
        setupPeriodicTimer(new PrintDiskUsageTimeout(),0, diskUsageTimeout);
    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        stats.incrementSentOps();
        stats.incrementReceivedGossip();
        UUID mid = request.getMsgId();
        byte[] content = request.getMsg();
        logger.info("SENT {}", mid);
        triggerNotification(new DeliverNotification(mid, content));
        GossipMessage msg = new GossipMessage(mid, myself, ++seqNumber, content);
        logger.debug("Accepted my op {}-{}: {}", myself, seqNumber, mid);
        handleGossipMessage(msg, myself);
    }

    private void uponUpdateStateRequest(UpdateStateRequest request, short sourceProto) {
        logger.debug("Received {}", request);
        this.stateAndVC.setState(request.getState());
        this.stateAndVC.setVC(request.getVc());
    }


    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveTreeMsg(TreeMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedTree();
        UUID mid = msg.getMid();
        logger.debug("Received tree {} from {}", mid, from);
        if (!receivedTreeIDs.contains(mid))
            handleTreeMessage(msg, from);
        else {
            stats.incrementReceivedDupesTree();
            logger.debug("{} was duplicated tree from {}", mid, from);
            StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-TREEDUPE: ", from));
            boolean print = false;

            if(partialView.contains(from)) { // Because we can receive messages before neigh up
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
                stats.incrementSentPrune();
            }

            if (print) {
                sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
                logger.info(sb);
            }
        }
    }

    private void uponReceiveGossipMsg(GossipMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedGossip();
        UUID mid = msg.getMid();
        logger.debug("Received gossip {} from {}", mid, from);
        if (!received.contains(mid)) {
            stats.incrementReceivedOps();
            Host h = msg.getOriginalSender();
            int clock = msg.getSenderClock();
            if (vectorClock.getHostClock(h) == clock - 1) {
                logger.debug("[{}] Accepted op {}-{} : {} from {}, Clock {}", false,
                        h, clock, mid, from, vectorClock.getHostClock(h));
                triggerNotification(new DeliverNotification(mid, msg.getContent()));
                handleGossipMessage(msg, from);
            } else if (vectorClock.getHostClock(h) < clock - 1) {
                logger.error("[{}] Out-of-order op {}-{} : {} from {}, Clock {}", false,
                        h, clock, mid, from, vectorClock.getHostClock(h));
            } else {
                logger.error("[{}] Ignored old op {}-{} : {} from {}, Clock {}", false,
                        h, clock, mid, from, vectorClock.getHostClock(h));
            }
        } else {
            stats.incrementReceivedDupesGossip();
            logger.info("DUPLICATE GOSSIP from {}", from);
        }
    }

    private void uponReceivePruneMsg(PruneMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedPrune();
        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-PRUNE: ", from));
        boolean print = false;

        if (eager.remove(from) != null) {
            logger.debug("Removed {} from eager due to prune {}", from, eager);
            print = true;
            sb.append(String.format("Removed %s from eager; ", from));

            if (lazy.add(from)) {
                logger.debug("Added {} to lazy due to prune {}", from, lazy);
                sb.append(String.format("Added %s to lazy; ", from));
            }
        }

        if (removeFromPendingIncomingSyncs(from)) {
            logger.debug("Removed {} from pendingIncomingSyncs due to prune {}", from, pendingIncomingSyncs);
            print = true;
            sb.append(String.format("Removed %s from pendingIncomingSyncs; ", from));
        }

        if (from.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from incomingSync due to prune", from);
            print = true;
            sb.append(String.format("Removed %s from incomingSync; ", from));
            tryNextIncomingSync();
        }

        if (print) {
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }
    }

    private void uponReceiveGraftMsg(GraftMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedGraft();
        logger.debug("Received {} from {}", msg, from);
        startOutgoingSync(from, UUID.randomUUID(), "GRAFT", false);
    }

    private void uponReceiveIHaveMsg(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedIHave();
        logger.debug("Received {} from {}", msg, from);
        handleAnnouncement(msg.getMid(), from);
    }

    private void uponReceiveVectorClockMsg(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedVC();
        logger.debug("Received {} from {}", msg, from);

        if(outgoingSyncs.contains(new OutgoingSync(from))) { // If sync was not cancelled

            VectorClock msgVC = msg.getVectorClock();
            StateAndVC stateAndVC = null;
            byte[] currState = this.stateAndVC.getState();
            if(currState != null && msgVC.isEmptyExceptFor(from)) {
                stateAndVC = new StateAndVC(this.stateAndVC.getState(), this.stateAndVC.getVc());
                logger.debug("Sending state {}", stateAndVC);
            }

            SynchronizationMessage synchronizationMsg = new SynchronizationMessage(msg.getMid(), stateAndVC,
                    this.fileManager.readSyncOpsFromFile(msgVC, vectorClock));
            sendMessage(synchronizationMsg, from);
            stats.incrementSentSyncOps();
            stats.incrementSentSyncGossipBy(synchronizationMsg.getMsgs().size());
            logger.debug("Sent {} to {}", synchronizationMsg, from);

            if (partialView.contains(from)) {
                StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-ENDSYNC: ", from));

                if (eager.put(from, msgVC) == null) {
                    logger.debug("Added {} to eager {} : pendingIncomingSyncs {}", from, eager.keySet(), pendingIncomingSyncs);
                    sb.append(String.format("Added %s to eager; ", from));
                }

                if (outgoingSyncs.remove(new OutgoingSync(from))) {
                    logger.debug("Removed {} from outgoingSyncs due to sync {}", from, outgoingSyncs);
                    sb.append(String.format("Removed %s from outgoingSyncs; ", from));
                }

                if (lazy.remove(from)) {
                    logger.debug("Removed {} from lazy due to sync {}", from, lazy);
                    sb.append(String.format("Removed %s from lazy; ", from));
                }

                sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
                logger.info(sb);

                sb = new StringBuilder("Cancelled timer for ");
                Iterator<Map.Entry<UUID, Queue<Host>>> iterator = missing.entrySet().iterator();
                while(iterator.hasNext()) {
                    Map.Entry<UUID, Queue<Host>> iHaves = iterator.next();
                    if (iHaves.getValue().contains(from)) {
                        cancelTimer(onGoingTimers.remove(iHaves.getKey()));
                        sb.append(iHaves.getKey()).append(" ");
                        iterator.remove();
                    }
                }
                logger.debug(sb);
            }
        }
    }

    private void uponReceiveSendVectorClockMsg(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedSendVC();
        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-SENDVC: ", from));

        UUID mid = msg.getMid();
        Host currentPending = incomingSync.getHost();

        if(currentPending == null) {
            incomingSync = new IncomingSync(from, mid);
            logger.debug("{} is my incomingSync ", from);
            sb.append(String.format("Added %s to incomingSync; ", from));
            VectorClockMessage vectorClockMessage = new VectorClockMessage(mid, new VectorClock(vectorClock.getClock()));
            sendMessage(vectorClockMessage, from, TCPChannel.CONNECTION_IN);
            stats.incrementSentVC();
            logger.debug("Sent {} to {}", vectorClockMessage, from);
        } else {
            pendingIncomingSyncs.add(new IncomingSync(from, mid));
            logger.debug("Added {} to pendingIncomingSyncs {}", from, pendingIncomingSyncs);
            sb.append(String.format("Added %s to pendingIncomingSyncs; ", from));
        }

        sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
        logger.info(sb);
    }

    private void uponReceiveSynchronizationMsg(SynchronizationMessage msg, Host from, short sourceProto, int channelId) {
        stats.incrementReceivedSyncOps();
        logger.debug("Received {} from {}", msg, from);

        StateAndVC msgStateAndVC = msg.getStateAndVC();
        if(msgStateAndVC != null)
            installStateAndExecuteSyncOps(msg, from, msgStateAndVC);
        else
            executeSyncOps(msg, from);

        logger.info("ENDED_SYNC {}",msg.getMid());
        tryNextIncomingSync();
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponSendTreeMessageTimeout(SendTreeMessageTimeout timeout, long timerId) {
        UUID mid = UUID.randomUUID();
        logger.debug("Generated tree msg {}", mid);
        handleTreeMessage(new TreeMessage(mid, myself), myself);
    }

    private void uponCheckReceivedTreeMessagesTimeout(CheckReceivedTreeMessagesTimeout timeout, long timerId) {
        if(treeMsgsFromSmallerHost == 0)
            sendTreeMsgTimer = setupPeriodicTimer(new SendTreeMessageTimeout(), 0, treeMsgTimeout);
        else
            treeMsgsFromSmallerHost = 0;
    }

    private void uponIHaveTimeout(IHaveTimeout timeout, long timerId) {
        UUID mid = timeout.getMid();
        logger.debug("IHave timeout {}", mid);
        if (onGoingTimers.containsKey(mid)) {
            if (!receivedTreeIDs.contains(mid)) {
                Host sender = missing.get(mid).poll();
                if (sender != null) {
                    logger.debug("Try sync with {} for timeout {}", sender, mid);
                    startOutgoingSync(sender, mid, "TIMEOUT-" + mid, true);
                }
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

    private void uponPrintDiskUsageTimeout(PrintDiskUsageTimeout timeout, long timerId) {
        logger.info("DiskUsage={}", this.fileManager.getCurrentDiskUsage());
    }


    /*--------------------------------- Notifications ---------------------------------------- */

    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        if (partialView.add(neighbour))
            logger.debug("Added {} to partial view due to up {}", neighbour, partialView);
        else
            logger.error("Tried to add {} to partial view but is already there {}", neighbour, partialView);

        openConnection(neighbour);
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        Host tmp = notification.getNeighbour();
        Host neighbour = new Host(tmp.getAddress(), tmp.getPort() + PORT_MAPPING);

        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-NEIGHDOWN: ", neighbour));
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
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }

        closeConnection(neighbour);
    }


    /* --------------------------------- Channel Events ---------------------------- */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.trace("Host {} is down, cause: {}", host, event.getCause());

        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-CONNDOWN: ", host));
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

        if (print) {
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }

        if (partialView.contains(host)) {
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
        }
    }

    @SuppressWarnings("rawtypes")
    private void uponOutConnectionFailed(OutConnectionFailed event, int channelId) {
        Host host = event.getNode();
        logger.trace("Connection to host {} failed, cause: {}", host, event.getCause());
        if(partialView.contains(host))
            setupTimer(new ReconnectTimeout(host), reconnectTimeout);
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host neighbour = event.getNode();
        logger.trace("Host (out) {} is up", neighbour);

        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-CONNUP: ", neighbour));

        if (partialView.contains(neighbour)) {
            if (lazy.add(neighbour)) {
                logger.debug("Added {} to lazy due to neigh up {}", neighbour, lazy);
                sb.append(String.format("Added %s to lazy; ", neighbour));
                sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
                logger.info(sb);
            }
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host host = event.getNode();
        logger.trace("Connection from host {} is down, cause: {}", host, event.getCause());

        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-INCONNDOWN: ", host));
        boolean print = false;

        if (removeFromPendingIncomingSyncs(host)) {
            logger.debug("Removed {} from pendingIncomingSyncs due to in connection down {}", host, pendingIncomingSyncs);
            print = true;
            sb.append(String.format("Removed %s from pendingIncomingSyncs; ", host));
        }

        if (host.equals(incomingSync.getHost())) {
            logger.debug("Removed {} from incomingSync due to in connection down", host);
            print = true;
            sb.append(String.format("Removed %s from incomingSync; ", host));
            tryNextIncomingSync();
        }

        if (print)
            logger.info(sb);
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void startOutgoingSync(Host neighbour, UUID msgId, String cause, boolean sendGraft) {
        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-STARTSYNC-", neighbour) + cause + ": ");
        OutgoingSync os = new OutgoingSync(neighbour, msgId);

        if (lazy.contains(neighbour) && !outgoingSyncs.contains(os)) {
            if (sendGraft) {
                logger.debug("Sent GraftMessage for {} to {}", msgId, neighbour);
                sendMessage(new GraftMessage(msgId), neighbour);
                stats.incrementSentGraft();
            }

            logger.debug("Added {} to outgoingSyncs", neighbour);
            outgoingSyncs.add(os);
            UUID mid = UUID.randomUUID();
            SendVectorClockMessage msg = new SendVectorClockMessage(mid);
            logger.info("STARTED_SYNC {}", mid);
            sendMessage(msg, neighbour);
            stats.incrementSentSendVC();
            logger.debug("Sent {} to {}", msg, neighbour);
            sb.append(String.format("Added %s to outgoingSyncs; ", neighbour));
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }
    }

    private void tryNextIncomingSync() {
        IncomingSync nextIncomingSync = pendingIncomingSyncs.poll();
        StringBuilder sb = new StringBuilder(String.format("[PEER %s] VIS-NEXTINCOMINGSYNC: ", incomingSync.getHost()));
        if (incomingSync.getHost() != null)
            sb.append(String.format("Removed %s from incomingSync; ", incomingSync.getHost()));

        if (nextIncomingSync != null) {
            Host currentPending = nextIncomingSync.getHost();
            incomingSync = nextIncomingSync;
            sb.append(String.format("Removed %s from pendingIncomingSyncs; ", currentPending));
            sb.append(String.format("Added %s to incomingSync; ", currentPending));
            VectorClockMessage vectorClockMessage = new VectorClockMessage(nextIncomingSync.getMid(), new VectorClock(vectorClock.getClock()));
            sendMessage(vectorClockMessage, currentPending, TCPChannel.CONNECTION_IN);
            stats.incrementSentVC();
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
            logger.debug("[PEER {}] Sent {} to {}", currentPending, vectorClockMessage, currentPending);
        } else {
            incomingSync = new IncomingSync(null, null);
            sb.append(String.format("VIEWS: eager %s lazy %s incomingSync %s pendingIncomingSyncs %s outgoingSyncs %s", eager.keySet(), lazy, incomingSync.getHost(), pendingIncomingSyncs, outgoingSyncs));
            logger.info(sb);
        }
    }

    private void handleTreeMessage(TreeMessage msg, Host from) {
        if (msg.getOriginalSender().compareTo(myself) <= 0) {
            logger.debug("Received tree msg from equal or smaller host {}", msg.getOriginalSender());
            treeMsgsFromSmallerHost++;
            if (!msg.getOriginalSender().equals(myself))
                cancelTimer(sendTreeMsgTimer);
        }

        UUID mid = msg.getMid();
        receivedTreeIDs.add(mid);

        Long tid;
        if ((tid = onGoingTimers.remove(mid)) != null) {
            cancelTimer(tid);
            missing.remove(mid);
        }

        StringBuilder sb = new StringBuilder(String.format("Forward tree %s received from %s to ", mid, from));
        for (Host peer : eager.keySet()) {
            if (!peer.equals(from)) {
                sendMessage(msg, peer);
                stats.incrementSentTree();
                sb.append(peer).append(" ");
            }
        }
        logger.debug(sb);

        IHaveMessage iHave = new IHaveMessage(mid);
        sb = new StringBuilder(String.format("Sent %s to ", iHave));
        for (Host peer : lazy) {
            if (!peer.equals(from)) {
                sendMessage(iHave, peer);
                stats.incrementSentIHave();
                sb.append(peer).append(" ");
            }
        }
        logger.debug(sb);
    }

    private void handleGossipMessage(GossipMessage msg, Host from) {
        logger.info("RECEIVED {}", msg.getMid());
        stats.incrementExecutedOps();
        vectorClock.incrementClock(msg.getOriginalSender());
        writeToFileAndForwardGossipMessage(msg, from);
    }

    private void writeToFileAndForwardGossipMessage(GossipMessage msg, Host from) {
        try {
            this.fileManager.writeOperationToFile(msg, vectorClock);
        } catch (IOException e) {
            logger.error("Error when writing operation to file", e);
        }

        UUID mid = msg.getMid();
        received.add(mid);
        for (Map.Entry<Host, VectorClock> entry : eager.entrySet()) {
            Host peer = entry.getKey();
            if (!peer.equals(from)) {
                VectorClock vc = entry.getValue();
                if (vc.getHostClock(msg.getOriginalSender()) < msg.getSenderClock()) {
                    sendMessage(msg, peer);
                    stats.incrementSentGossip();
                    logger.debug("Forward gossip {} received from {} to {}", mid, from, peer);
                }
            }
        }
    }

    private void handleAnnouncement(UUID mid, Host from) {
        if (!receivedTreeIDs.contains(mid)) {
            if (eager.isEmpty() && outgoingSyncs.isEmpty()) {
                logger.debug("Try sync with {} before timeout {}", from, mid);
                startOutgoingSync(from, mid, "BEFORETIMEOUT-" + mid, true);
            } else {
                if (!onGoingTimers.containsKey(mid)) {
                    long tid = setupTimer(new IHaveTimeout(mid), iHaveTimeout);
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

    private void installStateAndExecuteSyncOps(SynchronizationMessage msg, Host from, StateAndVC msgStateAndVC) {
        triggerNotification(new InstallStateNotification(msg.getMid(), msgStateAndVC.getState()));
        this.stateAndVC = msgStateAndVC;
        vectorClock = new VectorClock(msgStateAndVC.getVc().getClock());
        reexecuteMyOps();

        int nExecutedSyncOps = 0;
        for (byte[] serMsg : msg.getMsgs()) {
            stats.incrementReceivedSyncGossip();
            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serMsg));
                GossipMessage gossipMessage = GossipMessage.deserialize(dis);
                UUID mid = gossipMessage.getMid();
                Host h = gossipMessage.getOriginalSender();
                int msgClock = gossipMessage.getSenderClock();
                int myClock = vectorClock.getHostClock(h);
                if (myClock == msgClock - 1) {
                    logger.debug("[{}] Accepted op {}-{} : {} from {}, Clock {}", true, h, msgClock, mid,
                            from, myClock);
                    triggerNotification(new DeliverNotification(mid, gossipMessage.getContent()));
                    handleGossipMessage(gossipMessage, from);
                    nExecutedSyncOps++;
                } else if (myClock < msgClock - 1) {
                    logger.error("[{}] Out-of-order op {}-{} : {} from {}, Clock {}", true, h, msgClock,
                            mid, from, myClock);
                } else {
                    writeToFileAndForwardGossipMessage(gossipMessage, from);
                }
            } catch (IOException e) {
                logger.error("Sync message handling error", e);
            }
        }
        logger.debug("Executed {}/{} ops after installing state", nExecutedSyncOps, msg.getMsgs().size());
    }

    private void executeSyncOps(SynchronizationMessage msg, Host from) {
        for (byte[] serMsg : msg.getMsgs()) {
            stats.incrementReceivedSyncGossip();

            try {
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(serMsg));
                GossipMessage gossipMessage = GossipMessage.deserialize(dis);
                UUID mid = gossipMessage.getMid();

                if (!received.contains(mid)) {
                    stats.incrementReceivedOps();
                    Host h = gossipMessage.getOriginalSender();
                    int msgClock = gossipMessage.getSenderClock();
                    int myClock = vectorClock.getHostClock(h);
                    if (myClock == msgClock - 1) {
                        logger.debug("[{}] Accepted op {}-{} : {} from {}, Clock {}", true,
                                h, msgClock, mid, from, myClock);
                        triggerNotification(new DeliverNotification(mid, gossipMessage.getContent()));
                        handleGossipMessage(gossipMessage, from);
                    } else if (myClock < msgClock - 1) {
                        logger.error("[{}] Out-of-order op {}-{} : {} from {}, Clock {}", true,
                                h, msgClock, mid, from, myClock);
                    } else {
                        logger.error("[{}] Ignored old op {}-{} : {} from {}, Clock {}", true,
                                h, msgClock, mid, from, myClock);
                    }
                } else {
                    logger.info("DUPLICATE SYNC from {}", from);
                    logger.debug("Sync op {} was dupe", mid);
                    stats.incrementReceivedDupesSyncGossip();
                }
            } catch (IOException e) {
                logger.error("Sync message handling error", e);
            }
        }
    }

    private void reexecuteMyOps() {
        List<GossipMessage> myLateOps = this.fileManager.getMyLateOperations(myself, vectorClock, seqNumber);
        logger.debug("My VC pos is {} and my seqNumber is {}; executing {} ops", vectorClock.getHostClock(myself), seqNumber, myLateOps.size());
        for (GossipMessage msg : myLateOps) {
            UUID mid = msg.getMid();
            vectorClock.incrementClock(myself);
            logger.debug("Reexecuting {}-{} : {}", msg.getOriginalSender(), msg.getSenderClock(), mid);
            triggerNotification(new DeliverNotification(mid, msg.getContent()));
        }
    }
}