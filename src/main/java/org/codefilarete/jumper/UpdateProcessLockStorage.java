package org.codefilarete.jumper;

public interface UpdateProcessLockStorage {
	
	void insertRow(String identifier);
	
	void deleteRow(String identifier);
}
