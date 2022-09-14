package org.codefilarete.jumper.schema;

import java.util.Set;

public interface SequenceMetadataReader {
	
	Set<SequenceMetadata> giveSequences(String catalog, String schema);
}
