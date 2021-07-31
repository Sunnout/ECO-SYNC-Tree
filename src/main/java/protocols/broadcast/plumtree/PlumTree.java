package protocols.broadcast.plumtree;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.messages.SendVectorClockMessage;
import protocols.broadcast.common.messages.SyncOpsMessage;
import protocols.broadcast.common.messages.VectorClockMessage;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.plumtree.messages.*;
import protocols.broadcast.common.notifications.SendVectorClockNotification;
import protocols.broadcast.common.notifications.VectorClockNotification;
import protocols.broadcast.common.requests.SyncOpsRequest;
import protocols.broadcast.common.requests.VectorClockRequest;
import protocols.broadcast.common.timers.ReconnectTimeout;
import protocols.broadcast.plumtree.timers.IHaveTimeout;
import protocols.broadcast.plumtree.utils.AddressedIHaveMessage;
import protocols.broadcast.plumtree.utils.LazyQueuePolicy;
import protocols.broadcast.plumtree.utils.MessageSource;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.*;

public class PlumTree extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(PlumTree.class);

    public final static short PROTOCOL_ID = 900;
    public final static String PROTOCOL_NAME = "BCAST-PlumTree";

    protected int channelId;
    private final Host myself;
    private final static int PORT_MAPPING = 1000;

    private final long timeout1;
    private final long timeout2;
    private final long reconnectTimeout;
    private final boolean startInLazy;

    private final Set<Host> partialView;
    private final Set<Host> eager;
    private final Set<Host> lazy;
    private final Set<Host> onGoingSyncs; // Set to know which hosts we have asked for vcs
    private final Queue<Pair<Host, UUID>> pending;
    private Pair<Host, UUID> currentPendingInfo;
    private final Map<UUID, Queue<GossipMessage>> bufferedOps; //Buffer ops received between sending vc to kernel and sending sync ops (and send them after)
    private final Map<UUID, Queue<MessageSource>> missing;
    private final Set<UUID> received;
    private final Map<UUID, Long> onGoingTimers;
    private final Queue<AddressedIHaveMessage> lazyQueue;
    private final LazyQueuePolicy policy;

    /*** Stats ***/
    public static int sentGossip;
    public static int sentIHave;
    public static int sentGraft;
    public static int sentPrune;
    public static int sentSendVC;
    public static int sentVC;
    public static int sentSyncOps;
    public static int sentSyncGossip;

    public static int receivedGossip;
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
        this.timeout2 = Long.parseLong(properties.getProperty("timeout2", "500"));
        this.reconnectTimeout = Long.parseLong(properties.getProperty("reconnect_timeout", "500"));
        this.startInLazy = properties.getProperty("start_in_lazy", "false").equals("true");

        this.partialView = new HashSet<>();
        this.eager = new HashSet<>();
        this.lazy = new HashSet<>();
        this.onGoingSyncs = new HashSet<>();
        this.pending = new LinkedList<>();
        this.currentPendingInfo = Pair.of(null, null);
        this.bufferedOps = new HashMap<>();
        this.missing = new HashMap<>();
        this.received = new HashSet<>();
        this.onGoingTimers = new HashMap<>();
        this.lazyQueue = new LinkedList<>();
        this.policy = HashSet::new;

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
        registerTimerHandler(IHaveTimeout.TIMER_ID, this::uponIHaveTimeout);
        registerTimerHandler(ReconnectTimeout.TIMER_ID, this::uponReconnectTimeout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcast);
        registerRequestHandler(VectorClockRequest.REQUEST_ID, this::uponVectorClock);
        registerRequestHandler(SyncOpsRequest.REQUEST_ID, this::uponSyncOps);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_ID, GraftMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_ID, IHaveMessage.serializer);

        registerMessageSerializer(channelId, SendVectorClockMessage.MSG_ID, SendVectorClockMessage.serializer);
        registerMessageSerializer(channelId, VectorClockMessage.MSG_ID, VectorClockMessage.serializer);
        registerMessageSerializer(channelId, SyncOpsMessage.MSG_ID, SyncOpsMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
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

    }


    /*--------------------------------- Requests ---------------------------------------- */

    private void uponBroadcast(BroadcastRequest request, short sourceProto) {
        receivedGossip++;

        UUID mid = request.getMsgId();
        byte[] content = request.getMsg();
        logger.info("SENT {}", mid);
        logger.info("RECEIVED {}", mid);
        triggerNotification(new DeliverNotification(mid, myself, content, false));
        logger.debug("Propagating my {} to {}", mid, eager);
        GossipMessage msg = new GossipMessage(mid, myself, content);
        handleGossipMessage(msg, myself);
    }

    private void uponVectorClock(VectorClockRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        VectorClockMessage msg = new VectorClockMessage(request.getMsgId(), request.getSender(), request.getVectorClock());
        sendMessage(msg, neighbour, TCPChannel.CONNECTION_IN);
        sentVC++;
        logger.debug("Sent {} to {}", msg, neighbour);
    }

    private void uponSyncOps(SyncOpsRequest request, short sourceProto) {
        Host neighbour = request.getTo();
        UUID mid = request.getMsgId();
        SyncOpsMessage msg = new SyncOpsMessage(mid, request.getIds(), request.getOperations());
        sendMessage(msg, neighbour);
        sentSyncOps++;
        sentSyncGossip += request.getIds().size();
        logger.debug("Sent {} to {}", msg, neighbour);
        handleBufferedOperations(neighbour, mid);
        addNeighbourToEager(neighbour);
    }


    /*--------------------------------- Messages ---------------------------------------- */

    private void uponReceiveGossip(GossipMessage msg, Host from, short sourceProto, int channelId) {
        receivedGossip++;

        logger.debug("Received {} from {}", msg.getMid(), from);
        UUID mid = msg.getMid();
        if (!received.contains(mid)) {
            logger.info("RECEIVED {}", mid);
            triggerNotification(new DeliverNotification(mid, from, msg.getContent(), false));
            handleGossipMessage(msg, from);
        } else {
            receivedDupesGossip++;
            logger.info("DUPLICATE GOSSIP from {}", from);
            logger.debug("{} was duplicated msg from {}", mid, from);
            StringBuilder sb = new StringBuilder("VIS-DUPE: ");
            boolean print = false;

            if(partialView.contains(from)) { //Because we can receive messages before neigh up
                if (eager.remove(from)) {
                    logger.debug("Removed {} from eager due to duplicate {}", from, eager);
                    print = true;
                    sb.append(String.format("Removed %s from eager; ", from));
                }

                if (removeFromPending(from)) {
                    logger.debug("Removed {} from pending due to duplicate {}", from, pending);
                    print = true;
                    sb.append(String.format("Removed %s from pending; ", from));
                }

                if (from.equals(currentPendingInfo.getLeft())) {
                    logger.debug("Removed {} from current pending due to duplicate", from);
                    print = true;
                    sb.append(String.format("Removed %s from currPending; ", from));
                    tryNextSync();
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
                sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
                logger.info(sb);
            }
        }
    }

    private void uponReceivePrune(PruneMessage msg, Host from, short sourceProto, int channelId) {
        receivedPrune++;

        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder("VIS-PRUNE: ");

        if (eager.remove(from)) {
            logger.debug("Removed {} from eager due to prune {}", from, eager);
            sb.append(String.format("Removed %s from eager; ", from));

            if (lazy.add(from)) {
                logger.debug("Added {} to lazy due to prune {}", from, lazy);
                sb.append(String.format("Added %s to lazy; ", from));
            }

            sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
            logger.info(sb);
        }
    }

    private void uponReceiveGraft(GraftMessage msg, Host from, short sourceProto, int channelId) {
        receivedGraft++;

        logger.debug("Received {} from {}", msg, from);
        startSynchronization(from, false, "GRAFT");
    }

    private void uponReceiveIHave(IHaveMessage msg, Host from, short sourceProto, int channelId) {
        receivedIHave++;

        logger.debug("Received {} from {}", msg, from);
        handleAnnouncement(msg.getMid(), from);
    }

    private void uponReceiveVectorClock(VectorClockMessage msg, Host from, short sourceProto, int channelId) {
        receivedVC++;

        logger.debug("Received {} from {}", msg, from);
        this.bufferedOps.put(msg.getMid(), new LinkedList<>());
        triggerNotification(new VectorClockNotification(msg.getMid(), msg.getSender(), msg.getVectorClock()));
    }

    private void uponReceiveSendVectorClock(SendVectorClockMessage msg, Host from, short sourceProto, int channelId) {
        receivedSendVC++;

        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder("VIS-SENDVC: ");

        UUID mid = msg.getMid();
        if(partialView.contains(from)) {
            Host currentPending = currentPendingInfo.getLeft();
            if (currentPending == null) {
                currentPending = from;
                currentPendingInfo = Pair.of(currentPending, mid);
                logger.debug("{} is my currentPending ", from);
                sb.append(String.format("Added %s to currPending; ", from));
                triggerNotification(new SendVectorClockNotification(mid, from));
            } else {
                pending.add(Pair.of(from, mid));
                logger.debug("Added {} to pending {}", from, pending);
                sb.append(String.format("Added %s to pending; ", from));
            }
            sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
            logger.info(sb);
        } else
            triggerNotification(new SendVectorClockNotification(mid, from));
    }

    private void uponReceiveSyncOps(SyncOpsMessage msg, Host from, short sourceProto, int channelId) {
        receivedSyncOps++;

        logger.debug("Received {} from {}", msg, from);
        StringBuilder sb = new StringBuilder("VIS-SYNCOPS: ");

        Iterator<byte[]> opIt = msg.getOperations().iterator();
        Iterator<byte[]> idIt = msg.getIds().iterator();

        while (opIt.hasNext() && idIt.hasNext()) {
            receivedSyncGossip++;

            byte[] serOp = opIt.next();
            byte[] serId = idIt.next();
            UUID mid = deserializeId(serId);

            if (!received.contains(mid)) {
                logger.info("RECEIVED {}", mid);
                triggerNotification(new DeliverNotification(mid, from, serOp, true));
                logger.debug("Propagating sync op {} to {}", mid, eager);
                handleGossipMessage(new GossipMessage(mid, from, serOp), from);
            } else {
                logger.info("DUPLICATE SYNC from {}", from);
                logger.debug("Sync op {} was dupe", mid);
                receivedDupesSyncGossip++;
            }
        }
        sb.append(String.format("Removed %s from currPending; ", from));
        sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
        logger.info(sb);
        tryNextSync();
    }

    private void onMessageFailed(ProtoMessage protoMessage, Host host, short destProto, Throwable reason, int channel) {
        logger.warn("Message failed to " + host + ", " + protoMessage + ": " + reason.getMessage());
    }


    /*--------------------------------- Timers ---------------------------------------- */

    private void uponIHaveTimeout(IHaveTimeout timeout, long timerId) {
        UUID mid = timeout.getMid();
        if (!received.contains(mid)) {
            MessageSource msgSrc = missing.get(mid).poll();
            if (msgSrc != null) {
                long tid = setupTimer(timeout, timeout2);
                onGoingTimers.put(mid, tid);
                Host neighbour = msgSrc.peer;
                if (partialView.contains(neighbour) && !onGoingSyncs.contains(neighbour)) {
                    logger.debug("Sent GraftMessage for {} to {}", mid, neighbour);
                    sendMessage(new GraftMessage(mid), neighbour);
                    sentGraft++;
                }
                logger.debug("Try sync with {} for timeout {}", neighbour, mid);
                startSynchronization(neighbour, false, "TIMEOUT-" + mid);

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

        if (eager.remove(neighbour)) {
            logger.debug("Removed {} from eager due to down {}", neighbour, eager);
            print = true;
            sb.append(String.format("Removed %s from eager; ", neighbour));
        }

        if (lazy.remove(neighbour)) {
            logger.debug("Removed {} from lazy due to down {}", neighbour, lazy);
            print = true;
            sb.append(String.format("Removed %s from lazy; ", neighbour));
        }

        if (removeFromPending(neighbour)) {
            logger.debug("Removed {} from pending due to down {}", neighbour, pending);
            print = true;
            sb.append(String.format("Removed %s from pending; ", neighbour));
        }

        if (onGoingSyncs.remove(neighbour)) {
            logger.debug("Removed {} from onGoingSyncs due to down {}", neighbour, onGoingSyncs);
            print = true;
            sb.append(String.format("Removed %s from onGoingSyncs; ", neighbour));
        }

        MessageSource msgSrc = new MessageSource(neighbour);
        for (Queue<MessageSource> iHaves : missing.values()) {
            iHaves.remove(msgSrc);
        }

        if (neighbour.equals(currentPendingInfo.getLeft())) {
            logger.debug("Removed {} from current pending due to down", neighbour);
            print = true;
            sb.append(String.format("Removed %s from currPending; ", neighbour));
            tryNextSync();
        }

        if(print) {
            sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
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

        if (eager.remove(host)) {
            logger.debug("Removed {} from eager due to plumtree down {}", host, eager);
            print = true;
            sb.append(String.format("Removed %s from eager; ", host));
        }

        if (lazy.remove(host)) {
            logger.debug("Removed {} from lazy due to plumtree down {}", host, lazy);
            print = true;
            sb.append(String.format("Removed %s from lazy; ", host));
        }

        if (removeFromPending(host)) {
            logger.debug("Removed {} from pending due to plumtree down {}", host, pending);
            print = true;
            sb.append(String.format("Removed %s from pending; ", host));
        }

        if (onGoingSyncs.remove(host)) {
            logger.debug("Removed {} from onGoingSyncs due to plumtree down {}", host, onGoingSyncs);
            print = true;
            sb.append(String.format("Removed %s from onGoingSyncs; ", host));
        }

        if (host.equals(currentPendingInfo.getLeft())) {
            logger.debug("Removed {} from current pending due to plumtree down", host);
            print = true;
            sb.append(String.format("Removed %s from currPending; ", host));
            tryNextSync();
        }

        if(print) {
            sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
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
            if (startInLazy && !lazy.isEmpty()) {
                if (lazy.add(neighbour)) {
                    logger.debug("Added {} to lazy due to neigh up {}", neighbour, lazy);
                    sb.append(String.format("Added %s to lazy; ", neighbour));
                    sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
                    logger.info(sb);
                }
            } else {
                logger.debug("Trying sync from neighbour {} up", neighbour);
                startSynchronization(neighbour, true, "NEIGHUP");
            }
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from host {} is down, cause: {}", event.getNode(), event.getCause());
    }


    /*--------------------------------- Procedures ---------------------------------------- */

    private void startSynchronization(Host neighbour, boolean neighUp, String cause) {
        StringBuilder sb = new StringBuilder("VIS-STARTSYNC-" + cause + ": ");

        if (neighUp || (lazy.contains(neighbour) && !onGoingSyncs.contains(neighbour))) {
            logger.debug("Added {} to onGoingSyncs", neighbour);
            this.onGoingSyncs.add(neighbour);
            UUID mid = UUID.randomUUID();
            SendVectorClockMessage msg = new SendVectorClockMessage(mid);
            sendMessage(msg, neighbour);
            sentSendVC++;
            logger.debug("Sent {} to {}", msg, neighbour);
            sb.append(String.format("Added %s to onGoingSyncs; ", neighbour));
            sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
            logger.info(sb);
        }
    }

    private void addNeighbourToEager(Host neighbour) {
        StringBuilder sb = new StringBuilder("VIS-ENDSYNC: ");

        if (eager.add(neighbour)) {
            logger.debug("Added {} to eager {} : pending list {}", neighbour, eager, pending);
            sb.append(String.format("Added %s to eager; ", neighbour));
        }

        if (onGoingSyncs.remove(neighbour)) {
            logger.debug("Removed {} from onGoingSyncs due to sync {}", neighbour, onGoingSyncs);
            sb.append(String.format("Removed %s from onGoingSyncs; ", neighbour));
        }

        if (lazy.remove(neighbour)) {
            logger.debug("Removed {} from lazy due to sync {}", neighbour, lazy);
            sb.append(String.format("Removed %s from lazy; ", neighbour));
        }

        sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
        logger.info(sb);
    }

    private void tryNextSync() {
        StringBuilder sb = new StringBuilder("VIS-NEXTSYNC: ");
        Pair<Host, UUID> nextCurrentPendingInfo = pending.poll();

        if (nextCurrentPendingInfo != null) {
            Host currentPending = nextCurrentPendingInfo.getLeft();
            UUID mid = nextCurrentPendingInfo.getRight();
            currentPendingInfo = nextCurrentPendingInfo;
            logger.debug("{} is my currentPending try", currentPending);
            sb.append(String.format("Removed %s from pending; ", currentPending));
            sb.append(String.format("Added %s to currPending; ", currentPending));
            sb.append(String.format("VIEWS: eager %s lazy %s currPending %s pending %s onGoingSyncs %s", eager, lazy, currentPendingInfo.getLeft(), pending, onGoingSyncs));
            logger.info(sb);
            triggerNotification(new SendVectorClockNotification(mid, currentPending));
        } else {
            currentPendingInfo = Pair.of(null, null);
        }
    }

    private void handleGossipMessage(GossipMessage msg, Host from) {
        for(Queue<GossipMessage> q : this.bufferedOps.values())
            q.add(msg);

        UUID mid = msg.getMid();
        received.add(mid);

        Long tid;
        if ((tid = onGoingTimers.remove(mid)) != null) {
            cancelTimer(tid);
        }

        eagerPush(msg, from);
        lazyPush(msg, from);
    }

    private void handleBufferedOperations(Host neighbour, UUID mid) {
        Queue<GossipMessage> q = this.bufferedOps.remove(mid);
        GossipMessage msg;
        while ((msg = q.poll()) != null) {
            if (!msg.getSender().equals(neighbour)) {
                sendMessage(msg, neighbour);
                sentGossip++;
                logger.debug("Sent buffered {} to {}", msg, neighbour);
            }
        }
    }

    private void eagerPush(GossipMessage msg, Host from) {
        for (Host peer : eager) {
            if (!peer.equals(from)) {
                sendMessage(msg, peer);
                sentGossip++;
                logger.debug("Forward {} received from {} to {}", msg.getMid(), from, peer);
            }
        }
    }

    private void lazyPush(GossipMessage msg, Host from) {
        for (Host peer : lazy) {
            if (!peer.equals(from)) {
                lazyQueue.add(new AddressedIHaveMessage(new IHaveMessage(msg.getMid()), peer));
            }
        }
        dispatch();
    }

    private void handleAnnouncement(UUID mid, Host from) {
        if (!received.contains(mid)) {
            if (!onGoingTimers.containsKey(mid)) {
                long tid = setupTimer(new IHaveTimeout(mid), timeout1);
                onGoingTimers.put(mid, tid);
            }
            missing.computeIfAbsent(mid, v -> new LinkedList<>()).add(new MessageSource(from));
        }
    }

    private void dispatch() {
        Set<AddressedIHaveMessage> announcements = policy.apply(lazyQueue);
        for (AddressedIHaveMessage msg : announcements) {
            logger.debug("Sent {} to {}", msg.msg, msg.to);
            sendMessage(msg.msg, msg.to);
            sentIHave++;
        }
        lazyQueue.removeAll(announcements);
    }

    private UUID deserializeId(byte[] msg) {
        ByteBuf buf = Unpooled.buffer().writeBytes(msg);
        return new UUID(buf.readLong(), buf.readLong());
    }

    private boolean removeFromPending(Host host) {
        boolean removed = false;
        Iterator<Pair<Host, UUID>> it = this.pending.iterator();
        while(it.hasNext()) {
            if(it.next().getLeft().equals(host)) {
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