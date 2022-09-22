package org.codefilarete.jumper.schema.metadata;

import java.sql.JDBCType;

import org.codefilarete.jumper.schema.metadata.MetadataElement.TableNamespaceElementSupport;

public class ColumnMetadata extends TableNamespaceElementSupport implements MetadataElement {
	
	private String name;
	private JDBCType sqlType;
	private String vendorType;
	private Integer size;
	private Integer precision;
	private boolean nullable;
	private boolean autoIncrement;
	private int position;
	
	public ColumnMetadata(String catalog, String schema, String tableName) {
		super(catalog, schema, tableName);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public JDBCType getSqlType() {
		return sqlType;
	}
	
	public void setSqlType(JDBCType sqlType) {
		this.sqlType = sqlType;
	}
	
	public String getVendorType() {
		return vendorType;
	}
	
	public void setVendorType(String vendorType) {
		this.vendorType = vendorType;
	}
	
	public Integer getSize() {
		return size;
	}
	
	public void setSize(Integer size) {
		this.size = size;
	}
	
	public Integer getPrecision() {
		return precision;
	}
	
	public void setPrecision(Integer precision) {
		this.precision = precision;
	}
	
	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public boolean isAutoIncrement() {
		return autoIncrement;
	}
	
	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}
	
	public int getPosition() {
		return position;
	}
	
	public void setPosition(int position) {
		this.position = position;
	}
	
	@Override
	public String toString() {
		return "Column{" +
				"name='" + name + '\'' +
				", sqlType=" + sqlType +
				", vendorType='" + vendorType + '\'' +
				", size=" + size +
				", nullable=" + nullable +
				", autoIncrement=" + autoIncrement +
				'}';
	}
}
