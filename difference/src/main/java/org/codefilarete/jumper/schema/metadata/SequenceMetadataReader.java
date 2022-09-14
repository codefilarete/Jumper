package org.codefilarete.jumper.schema.metadata;

import java.util.Set;

public interface SequenceMetadataReader {
	
	Set<SequenceMetadata> giveSequences(String catalog, String schema);
}
