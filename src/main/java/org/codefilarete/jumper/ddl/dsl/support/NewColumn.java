package org.codefilarete.jumper.ddl.dsl.support;

/**
 * @author Guillaume Mary
 */
public class NewColumn {
	
	private final String name;
	private final String sqlType;
	private boolean nullable = true;
	private String defaultValue;
	private boolean autoIncrement = false;
	private String uniqueConstraintName;
	
	public NewColumn(String name, String sqlType) {
		this.name = name;
		this.sqlType = sqlType;
	}
	
	public String getName() {
		return name;
	}
	
	public String getSqlType() {
		return sqlType;
	}
	
	public boolean isNullable() {
		return nullable;
	}
	
	public void notNull() {
		setNullable(false);
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public String getDefaultValue() {
		return defaultValue;
	}
	
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	public boolean isAutoIncrement() {
		return autoIncrement;
	}
	
	public void autoIncrement() {
		setAutoIncrement(true);
	}
	
	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}
	
	public String getUniqueConstraint() {
		return uniqueConstraintName;
	}
	
	public void setUniqueConstraint(String name) {
		this.uniqueConstraintName = name;
	}
}
