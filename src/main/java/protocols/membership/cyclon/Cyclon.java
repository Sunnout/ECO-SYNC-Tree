package protocols.membership.cyclon;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.ChannelMetrics;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.cyclon.components.Connection;
import protocols.membership.cyclon.messages.SampleMessage;
import protocols.membership.cyclon.messages.SampleMessageReply;
import protocols.membership.cyclon.timers.InfoTimer;
import protocols.membership.cyclon.timers.SampleTimer;

public class Cyclon extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(Cyclon.class);

	// Protocol information, to register in babel
	public final static short PROTOCOL_ID = 1000;
	public final static String PROTOCOL_NAME = "CyclonMembership";

	private final Host self; // My own address/port
	private final Set<Host> membership; // Peers I am connected to
	private final Set<Host> pending; // Peers I am trying to connect to
	private final Map<Host, Integer> membersAge;
	private final Map<Host, List<Connection>> pendingSampleReplies;
	private Host lastSentHost;
	private List<Connection> lastSentSample;

	// Protocol parameters
	private final int sampleTime; // Timeout for samples
	private final int subsetSize; // Maximum size of sample
	
	private int messagesSent;
	private long bytesSent;

	private final int channelId;


	public Cyclon(Properties props, Host self) throws IOException, HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.self = self;
		this.membership = new HashSet<>();
		this.pending = new HashSet<>();
		this.membersAge = new HashMap<>();
		this.lastSentHost = null;
		this.lastSentSample = null;
		this.pendingSampleReplies = new HashMap<>();

		// Get some configurations from properties file
		this.subsetSize = Integer.parseInt(props.getProperty("sample_size", "6"));
		this.sampleTime = Integer.parseInt(props.getProperty("sample_time", "2000")); // 2 seconds
		
		// Message counters
		this.messagesSent = 0;

		String cMetricsInterval = props.getProperty("channel_metrics_interval", "20000"); // 20 seconds

		// Create a properties object to setup channel-specific properties
		Properties channelProps = new Properties();
		channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); // The address to bind to
		channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); // The port to bind to
		channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); // The interval to receive channel metrics
		channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); // Heartbeats interval for established connections
		channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); // Time passed without heartbeats until closing a connection
		channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); // TCP connect timeout
		channelId = createChannel(TCPChannel.NAME, channelProps); // Create the channel with the given properties

		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(channelId, SampleMessage.MSG_ID, SampleMessage.serializer);
		registerMessageSerializer(channelId, SampleMessageReply.MSG_ID, SampleMessageReply.serializer);

		/*---------------------- Register Message Handlers -------------------------- */
		registerMessageHandler(channelId, SampleMessage.MSG_ID, this::uponShuffle, this::uponMsgFail);
		registerMessageHandler(channelId, SampleMessageReply.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

		/*--------------------- Register Timer Handlers ----------------------------- */
		registerTimerHandler(SampleTimer.TIMER_ID, this::uponShuffleTimer);
		registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

		/*-------------------- Register Channel Events ------------------------------- */
		registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
		registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
		registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
	}

	@Override
	public void init(Properties props) {

		// Inform the dissemination protocol about the channel we created
		triggerNotification(new ChannelCreated(channelId));

		// If there is a contact node, attempt to establish connection
		if (props.containsKey("contact")) {
			try {
				String contact = props.getProperty("contact");
				String[] hostElems = contact.split(":");
				Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
				// We add to the pending set until the connection is successful
				pending.add(contactHost);
				openConnection(contactHost);
			} catch (Exception e) {
				logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
				e.printStackTrace();
				System.exit(-1);
			}
		}

		// Setup the timer used to send samples
		setupPeriodicTimer(new SampleTimer(), this.sampleTime, this.sampleTime);

		// Setup the timer to display protocol information
		int pMetricsInterval = Integer.parseInt(props.getProperty("protocol_metrics_interval", "10000"));
		if (pMetricsInterval > 0)
			setupPeriodicTimer(new InfoTimer(), pMetricsInterval, pMetricsInterval);
	}

	
	/*--------------------------------- Messages ---------------------------------------- */
	
	// When the SampleTimer is triggered, gets the oldest peer in the membership and sends a sample
	private void uponShuffleTimer(SampleTimer timer, long timerId) {
		if (membership.size() > 0) {
			increaseAge(membership);
			Host target = getOldest(membership);
			sendShuffle(target);
			removeMembership(target);
		}
	}

	// Sends a shuffle to the target
	private void sendShuffle(Host target) {
		Set<Host> subset = getRandomSubsetExcluding(membership, subsetSize, target);
		List<Connection> sample = new ArrayList<>();

		for (Host h : subset)
			sample.add(new Connection(h, membersAge.get(h)));

		sample.add(new Connection(self, 0));
		SampleMessage message = new SampleMessage(sample);
		sendMessage(message, target);
		messagesSent++;
		lastSentSample = sample;
		lastSentHost = target;
	}

	// Called when the process receives a shuffle request from another process
	private void uponShuffle(SampleMessage msg, Host from, short sourceProto, int channelId) {
		Set<Host> hostsSample = getRandomSubsetExcluding(membership, subsetSize, from);
		List<Connection> sample = new ArrayList<>();

		for (Host h : hostsSample)
			sample.add(new Connection(h, membersAge.get(h)));

		sendShuffleReply(from, sample);
		mergeViews(msg.getSample(), sample);
	}

	// Sends a shuffle reply to the process that sent a shuffle request
	private void sendShuffleReply(Host target, List<Connection> sample) {
		if (membership.contains(target)) {
			SampleMessageReply message = new SampleMessageReply(sample);
			sendMessage(message, target);
			messagesSent++;
		} else
			pendingSampleReplies.put(target, sample);
	}

	// The process received a shuffle reply from another process
	private void uponShuffleReply(SampleMessageReply msg, Host from, short sourceProto, int channelId) {
		if (from.equals(lastSentHost)) {
			mergeViews(msg.getSample(), lastSentSample);
			lastSentHost = null;
			lastSentSample.clear();
		} else
			logger.debug("{} from {} is late, skipping sample reply", msg, from);
	}

	/*---------------------------- Membership Management ------------------------------ */

	// Deals with management of memberships when a sample is received from another process
	private void mergeViews(List<Connection> peerSample, List<Connection> mySample) {
		for (Connection connection : peerSample) {
			Host h = connection.getHost();
			int hostSampleAge = connection.getAge();

			if (membership.contains(h)) {
				if (membersAge.get(h) > hostSampleAge)
					membersAge.put(h, hostSampleAge);
			} 
			else if (membership.size() < subsetSize)
				startMembership(connection);
			else {
				Host toRemove = pickHostInBoth(membership, mySample);

				if (toRemove == null)
					toRemove = pickRandomHost(membership);

				removeMembership(toRemove);
				startMembership(connection);
			}
		}
	}

	// Removes a process from the membership
	private void removeMembership(Host host) {
		closeConnection(host);
		membership.remove(host);
		membersAge.remove(host);
		triggerNotification(new NeighbourDown(host));
	}

	// Adds a process to the membership
	private void startMembership(Connection host) {
		Host h = host.getHost();
		openConnection(h);
		pending.add(h);
		membersAge.put(h, host.getAge());
	}

	
	/*------------------------------ Auxiliary Methods -------------------------------- */

	// Gets the oldest known host
	// Always returns non null because ageMembership() is called before the first time this method is called
	private Host getOldest(Set<Host> hostSet) {
		Host toReturn = null;
		int currentAge = 0;
		int age;

		for (Host h : hostSet) {
			age = membersAge.get(h);
			if (age > currentAge) {
				toReturn = h;
				currentAge = age;
			}
		}

		return toReturn;
	}

	// Increments the age of the whole known membership
	private void increaseAge(Set<Host> hostSet) {
		for (Host h : hostSet)
			membersAge.put(h, membersAge.get(h) + 1);
	}

	// Gets a random subset from the set of peers
	private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
		List<Host> list = new LinkedList<>(hostSet);
		list.remove(exclude);
		Collections.shuffle(list);
		return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
	}

	// Picks a random peer that is contained in both sets
	private Host pickHostInBoth(Set<Host> set1, List<Connection> set2) {
		for (Connection connection : set2) {
			Host h = connection.getHost();
			if (set1.contains(h))
				return h;
		}

		return null;
	}

	// Picks a random host from a set received as parameter
	private Host pickRandomHost(Set<Host> setToSearch) {
		List<Host> list = new LinkedList<>(setToSearch);
		Collections.shuffle(list);
		return list.get(0);
	}

	/* ---------------------------- TCPChannel Events ---------------------------- */

	// If a connection is successfully established, this event is triggered. In this protocol, we want to add the
	// respective peer to the membership, and inform the Dissemination protocol via a notification.
	private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
		Host peer = event.getNode();
		pending.remove(peer);
		if (membership.add(peer)) {
			triggerNotification(new NeighbourUp(peer));
			logger.debug("Membership: {}", membership);

			if (!membersAge.containsKey(peer))
				membersAge.put(peer, 0);

			if (pendingSampleReplies.containsKey(peer)) {
				List<Connection> sample = pendingSampleReplies.remove(peer);
				sendShuffleReply(peer, sample);
			}
		}
	}

	// Opens connections to a set of hosts and adds them to the pending set
	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	// If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
	// protocol. Alternatively, we could do smarter things like retrying the connection X times.
	private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
		Host peer = event.getNode();
		logger.debug("Connection to {} is down cause {}", peer, event.getCause());
		membership.remove(peer);
		membersAge.remove(peer);
		triggerNotification(new NeighbourDown(peer));
	}

	// If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
	// pending set. Note that this event is only triggered while attempting a connection, not after connection.
	// Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
		logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
		pending.remove(event.getNode());
		membersAge.remove(event.getNode());
	}

	// If someone established a connection to me, this event is triggered. In this protocol we do nothing with this 
	// event. If we want to add the peer to the membership, we will establish our own outgoing connection.
	// (not the smartest protocol, but its simple)
	private void uponInConnectionUp(InConnectionUp event, int channelId) {
		logger.trace("Connection from {} is up", event.getNode());
	}

	// A connection someone established to me is disconnected.
	private void uponInConnectionDown(InConnectionDown event, int channelId) {
		logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
	}

	/* --------------------------------- Metrics ---------------------------- */

	// If we setup the InfoTimer in the constructor, this event will be triggered periodically.
	// We are simply printing some information to present during runtime.
	private void uponInfoTime(InfoTimer timer, long timerId) {
		StringBuilder sb = new StringBuilder("Membership Metrics:\n");
		sb.append("Membership: ").append(membership).append("\n");
		sb.append("PendingMembership: ").append(pending).append("\n");
		// getMetrics returns an object with the number of events of each type processed
		// by this protocol.
		// It may or may not be useful to you, but at least you know it exists.
		sb.append(getMetrics());
		logger.debug(sb);
	}

	// If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
	// periodically by the channel. This is NOT a protocol timer, but a channel event.
	// Again, we are just showing some of the information you can get from the channel, and use how you see fit.
	// "getInConnections" and "getOutConnections" returns the currently established connection to/from me.
	// "getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
	private void uponChannelMetrics(ChannelMetrics event, int channelId) {
		StringBuilder sb = new StringBuilder("Channel Metrics:\n");
		long bytesSent = 0;
		long bytesReceived = 0;
		long messagesSent = 0;
		long messagesReceived = 0;

		for(ChannelMetrics.ConnectionMetrics c: event.getOutConnections()){
			bytesSent += c.getSentAppBytes();
			messagesSent += c.getSentAppMessages();
		}

		for(ChannelMetrics.ConnectionMetrics c: event.getOldOutConnections()){
			bytesSent += c.getSentAppBytes();
			messagesSent += c.getSentAppMessages();
		}

		for(ChannelMetrics.ConnectionMetrics c: event.getInConnections()){
			bytesReceived += c.getReceivedAppBytes();
			messagesReceived += c.getReceivedAppMessages();
		}

		for(ChannelMetrics.ConnectionMetrics c: event.getOldInConnections()){
			bytesReceived += c.getReceivedAppBytes();
			messagesReceived += c.getReceivedAppMessages();
		}

		sb.append(String.format("BytesSent = %s\n", bytesSent));
		sb.append(String.format("BytesReceived = %s\n", bytesReceived));
		sb.append(String.format("MessagesSent = %s\n", messagesSent));
		sb.append(String.format("MessagesReceived = %s\n", messagesReceived));
		sb.setLength(sb.length() - 1);
		logger.info(sb);
	}
}