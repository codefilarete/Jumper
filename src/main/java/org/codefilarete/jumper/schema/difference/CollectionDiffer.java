package org.codefilarete.jumper.schema.difference;

import java.util.Collection;

import org.codefilarete.tool.collection.KeepOrderSet;

public interface CollectionDiffer<T, C extends Collection<T>, D extends AbstractDiff<T>> {

	KeepOrderSet<D> diff(C before, C after);
}
