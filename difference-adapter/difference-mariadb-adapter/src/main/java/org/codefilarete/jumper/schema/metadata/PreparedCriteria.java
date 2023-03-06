package org.codefilarete.jumper.schema.metadata;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PreparedCriteria {
	
	static PreparedCriteria asSQLCriteria(String columnName, Operator<?> operator) {
		if (operator == null) {
			return null;
		}
		String criteriaSegment = null;
		Collection<String> values = null;
		if (operator instanceof Like) {
			criteriaSegment = columnName + " like ?";
			values = Arrays.asList((String) operator.getValue());
		} else if (operator instanceof Equal) {
			criteriaSegment = columnName + " = ?";
			values = Arrays.asList((String) operator.getValue());
		} else if (operator instanceof In) {
			criteriaSegment = columnName + " in (" + IntStream.of(((Set<String>) operator.getValue()).size()).mapToObj(i -> "?").collect(Collectors.joining(", ")) + ")";
			values = (Set<String>) operator.getValue();
		}
		return new PreparedCriteria(criteriaSegment, values);
	}
	
	private final String criteriaSegment;
	
	private final Collection<String> values;
	
	public PreparedCriteria(String criteriaSegment, Collection<String> values) {
		this.criteriaSegment = criteriaSegment;
		this.values = values;
	}
	
	public String getCriteriaSegment() {
		return criteriaSegment;
	}
	
	public Collection<String> getValues() {
		return values;
	}
	
	public static abstract class Operator<V> {
		
		protected V value;
		
		public Operator(V value) {
			this.value = value;
		}
		
		public V getValue() {
			return value;
		}
	}
	
	public static class Like<V extends String> extends Operator<V> {
		
		public Like(V value) {
			super(value);
		}
		
	}
	
	public static class Equal<V extends String> extends Operator<V> {
		
		public Equal(V value) {
			super(value);
		}
	}
	
	public static class In<V extends String> extends Operator<Set<V>> {
		
		public In(Set<V> value) {
			super(value);
		}
	}
}
