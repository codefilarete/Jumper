package org.gama.jumper;

/**
 * Class aimed at helping to determine if an {@link Update} has changed. Based on {@link SignableUpdate}.
 * 
 * @author Guillaume Mary
 */
public class VersatileUpdateSupport {
	
	private final ApplicationUpdatesStorage applicationUpdatesStorage;
	
	public VersatileUpdateSupport(ApplicationUpdatesStorage applicationUpdatesStorage) {
		this.applicationUpdatesStorage = applicationUpdatesStorage;
	}
	
	public boolean hasChanged(SignableUpdate signableUpdate) {
		SignableUpdate update = applicationUpdatesStorage.get(signableUpdate.getIdentifier(), SignableUpdate.class);
		return update != null && !update.getSignature().equals(signableUpdate.getSignature());
	}
}
