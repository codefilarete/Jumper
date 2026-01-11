package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;

import org.codefilarete.jumper.schema.metadata.MetadataElement.SchemaNamespaceElementSupport;

public class ProcedureMetadata extends SchemaNamespaceElementSupport implements MetadataElement {
	
	/**
	 * kind of procedure
	 * @author Guillaume Mary
	 */
	public enum ProcedureType {
		ROUTINE(DatabaseMetaData.procedureResultUnknown),	// Cannot determine if a return value will be returned
		PROCEDURE(DatabaseMetaData.procedureNoResult),	// Does not return a return value
		FUNCTION(DatabaseMetaData.procedureReturnsResult);	// Returns a return value
		
		private final int jdbcValue;
		
		ProcedureType(int jdbcValue) {
			this.jdbcValue = jdbcValue;
		}
		
		public int getJdbcValue() {
			return jdbcValue;
		}
		
		private static final ProcedureType[] VALUES_PER_JDBC_VALUE_ORDER = new ProcedureType[] { ROUTINE, PROCEDURE, FUNCTION };
		
		public static ProcedureType valueOf(short resultSetValue) {
			return VALUES_PER_JDBC_VALUE_ORDER[resultSetValue];
		}
	}
	
	private final String name;
	private String remarks;
	private ProcedureType type;
	private String specificName;
	
	public ProcedureMetadata(String catalog, String schema, String name) {
		super(catalog, schema);
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public String getRemarks() {
		return remarks;
	}
	
	public void setRemarks(String remarks) {
		this.remarks = remarks;
	}
	
	public ProcedureType getType() {
		return type;
	}
	
	public void setType(ProcedureType type) {
		this.type = type;
	}
	
	public String getSpecificName() {
		return specificName;
	}
	
	public void setSpecificName(String specificName) {
		this.specificName = specificName;
	}
	
	@Override
	public String toString() {
		return "ProcedureMetadata{" +
				"name='" + name + '\'' +
				", remarks='" + remarks + '\'' +
				", type=" + type +
				", specificName='" + specificName + '\'' +
				'}';
	}
}
