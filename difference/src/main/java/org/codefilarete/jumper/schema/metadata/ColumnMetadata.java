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
	
	public ColumnMetadata setName(String name) {
		this.name = name;
		return this;
	}
	
	public JDBCType getSqlType() {
		return sqlType;
	}
	
	public ColumnMetadata setSqlType(JDBCType sqlType) {
		this.sqlType = sqlType;
		return this;
	}
	
	public String getVendorType() {
		return vendorType;
	}
	
	public ColumnMetadata setVendorType(String vendorType) {
		this.vendorType = vendorType;
		return this;
	}
	
	public Integer getSize() {
		return size;
	}
	
	public ColumnMetadata setSize(Integer size) {
		this.size = size;
		return this;
	}
	
	public Integer getPrecision() {
		return precision;
	}
	
	public ColumnMetadata setPrecision(Integer precision) {
		this.precision = precision;
		return this;
	}
	
	public boolean isNullable() {
		return nullable;
	}
	
	public ColumnMetadata setNullable(boolean nullable) {
		this.nullable = nullable;
		return this;
	}
	
	public boolean isAutoIncrement() {
		return autoIncrement;
	}
	
	public ColumnMetadata setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
		return this;
	}
	
	public int getPosition() {
		return position;
	}
	
	public ColumnMetadata setPosition(int position) {
		this.position = position;
		return this;
	}
	
	@Override
	public String toString() {
		return "Column{" +
				"catalog='" + getCatalog() + '\'' +
				", schema='" + getSchema() + '\'' +
				", table='" + getTableName() + '\'' +
				", name='" + name + '\'' +
				", sqlType=" + sqlType +
				", vendorType='" + vendorType + '\'' +
				", size=" + size +
				", precision=" + precision +
				", nullable=" + nullable +
				", autoIncrement=" + autoIncrement +
				", position=" + position +
				'}';
	}
}
