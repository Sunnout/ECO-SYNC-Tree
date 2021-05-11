package protocols.broadcast.eagerpush.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class ClearReceivedIdsTimer extends ProtoTimer {
	public static final short TIMER_ID = 604;
	
	public ClearReceivedIdsTimer() {
		super(TIMER_ID);
	}

	@Override
	public ProtoTimer clone() {
		return this;
	}
}