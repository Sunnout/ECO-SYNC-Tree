package protocols.broadcast.eagerpush;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocols.broadcast.eagerpush.timers.ClearReceivedIdsTimer;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import protocols.broadcast.common.requests.BroadcastRequest;
import protocols.broadcast.common.notifications.DeliverNotification;
import protocols.broadcast.eagerpush.messages.EagerPushMessage;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

public class EagerPushBroadcast extends GenericProtocol {
	private static final Logger logger = LogManager.getLogger(EagerPushBroadcast.class);

	// Protocol information, to register in babel
	public static final String PROTOCOL_NAME = "EagerPush";
	public static final short PROTOCOL_ID = 500;

	private final Host myself; // My own address/port
	private final Set<Host> neighbours; // Set of known neighbours
	private final Map<UUID, Long> receivedTimes; // Map of <msgIds, receivedTimes>
	
	// Protocol parameters
	private final int clearReceivedIds; // Timeout to clear received messages
	private final int fanout; // Number of neighbours to send gossip message to

	private boolean channelReady;

	public EagerPushBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
		super(PROTOCOL_NAME, PROTOCOL_ID);
		this.myself = myself;
		neighbours = new HashSet<>();
		receivedTimes = new HashMap<>();
		channelReady = false;
		
		// Get some configurations from properties file
		clearReceivedIds = Integer.parseInt(properties.getProperty("clear_ids", "90000"));
		fanout = Integer.parseInt(properties.getProperty("fanout", "2"));

		/*--------------------- Register Request Handlers ----------------------------- */
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

		/*--------------------- Register Notification Handlers ------------------------- */
		subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
		subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
		subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);

		/*--------------------- Register Timer Handlers ------------------------- */
		registerTimerHandler(ClearReceivedIdsTimer.TIMER_ID, this::uponClearReceivedIdsTimer);
	}

	@Override
	public void init(Properties props) {
		setupPeriodicTimer(new ClearReceivedIdsTimer(), clearReceivedIds, clearReceivedIds);
	}

	// Upon receiving the channelId from the membership, register callbacks and serializers
	private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
		int cId = notification.getChannelId();
		registerSharedChannel(cId);
		
		/*---------------------- Register Message Serializers ---------------------- */
		registerMessageSerializer(cId, EagerPushMessage.MSG_ID, EagerPushMessage.serializer);

		/*---------------------- Register Message Handlers -------------------------- */
		try {
			registerMessageHandler(cId, EagerPushMessage.MSG_ID, this::uponEagerPushMessage, this::uponMsgFail);
		} catch (HandlerRegistrationException e) {
			logger.error("Error registering message handler: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
		channelReady = true;
	}

	
	/*--------------------------------- Requests ---------------------------------------- */
	
	private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
		if (!channelReady)
			return;

		EagerPushMessage msg = new EagerPushMessage(request.getMsgId(), request.getSender(), sourceProto,
				request.getMsg());

		// Call the same handler as when receiving a new EagerPushMessage
		uponEagerPushMessage(msg, myself, getProtoId(), -1);
	}

	
	/*--------------------------------- Messages ---------------------------------------- */
	
	private void uponEagerPushMessage(EagerPushMessage msg, Host from, short sourceProto, int channelId) {
		UUID msgId = msg.getMid();
		long receivedTime = System.currentTimeMillis();
		
		// If we already received it once, do nothing
		if (!receivedTimes.containsKey(msgId)) {
			receivedTimes.put(msgId, receivedTime);
			triggerNotification(new DeliverNotification(msg.getMid(), msg.getSender(), msg.getContent(), false));

			Set<Host> sample = getRandomSubsetExcluding(neighbours, fanout, from);
			// Simply send the message to a subset of size fanout of known neighbours
			sample.forEach(host -> {
				sendMessage(msg, host);
			});
		}
	}

	private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
		logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
	}

	
	/*-------------------------------------- Timers ----------------------------------------- */

	// Clear message IDs periodically (longer timeout)
	private void uponClearReceivedIdsTimer(ClearReceivedIdsTimer clearReceivedIdsTimer, long timerId) {
		Iterator<Entry<UUID, Long>> it = receivedTimes.entrySet().iterator();
		while (it.hasNext()) {
			Entry<UUID, Long> entry = it.next();
			if(System.currentTimeMillis() > receivedTimes.get(entry.getKey()) + clearReceivedIds) {
				it.remove();
			}
		}
	}

	/*--------------------------------- Notifications ---------------------------------------- */

	private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
		Host h = notification.getNeighbour();
		neighbours.add(h);
		logger.debug("New neighbour: " + h);
	}

	private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
		Host h = notification.getNeighbour();
		neighbours.remove(h);
		logger.debug("Neighbour down: " + h);
	}
	
	
	/*----------------------------------- Procedures ----------------------------------------- */

	private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
		List<Host> list = new LinkedList<>(hostSet);
		list.remove(exclude);
		Collections.shuffle(list);
		return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
	}

}
