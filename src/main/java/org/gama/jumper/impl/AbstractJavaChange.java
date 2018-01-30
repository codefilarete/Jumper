package org.gama.jumper.impl;

import org.gama.jumper.AbstractChange;
import org.gama.jumper.Checksum;
import org.gama.jumper.ChangeId;

/**
 * An update dedicated to Java code execution.
 * Be aware that by default its Checksum depends of the class bytecode. So every code change will make it change too, hence it will considered
 * non-compliant (error will be raised) when re-applied onto an system. See {@link #computeChecksum()} for more details. 
 * 
 * @author Guillaume Mary
 */
public abstract class AbstractJavaChange extends AbstractChange {
	
	public AbstractJavaChange(ChangeId changeId, boolean shouldAlwaysRun) {
		super(changeId, shouldAlwaysRun);
	}
	
	public AbstractJavaChange(String identifier, boolean shouldAlwaysRun) {
		this(new ChangeId(identifier), shouldAlwaysRun);
	}
	
	/**
	 * Implemented to compute the Checksum of this class.
	 * Therefore it depends on this class bytecode. This makes it depends of every code modification (except comments, some spaces, line breaks, etc).
	 * Thus this principle has some drawbacks :
	 * - it doesn't take dependency to other classes into account
	 * - even code modification of non business logic (outside execute method) alters it
	 * - compiler and Java version may also alters it
	 * 
	 * So it must be considered as a best effort / minimal behavior and is far from perfect.  
	 * 
	 * @return a {@link Checksum} of SQL orders
	 */
	@Override
	public Checksum computeChecksum() {
		return ClassChecksumer.INSTANCE.checksum(this.getClass());
	}
	
}
