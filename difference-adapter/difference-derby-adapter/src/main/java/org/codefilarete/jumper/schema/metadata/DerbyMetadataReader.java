package org.codefilarete.jumper.schema.metadata;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Set;

import org.codefilarete.jumper.schema.metadata.ProcedureMetadata.ProcedureType;
import org.codefilarete.tool.Strings;

public class DerbyMetadataReader extends DefaultMetadataReader {
	
	public DerbyMetadataReader(DatabaseMetaData metaData) {
		super(metaData);
	}
	
	/**
	 * {@inheritDoc}
	 *
	 * Overridden to align behavior with other vendors by setting empty Remarks to null
	 */
	@Override
	protected Set<TableMetadata> giveTables(String catalog, String schema, String tableNamePattern, String[] tableTypes) {
		Set<TableMetadata> tables = super.giveTables(catalog, schema, tableNamePattern, tableTypes);
		// set empty Remarks to null from the result (to align the behavior with other vendors)
		tables.forEach(tableMetadata -> {
			if (Strings.isEmpty(tableMetadata.getRemarks())) {
				tableMetadata.setRemarks(null);
			}
		});
		return tables;
	}
	
	/**
	 * Sets null catalog for function procedures.
	 * For unknown reasons, Derby gives 'null' for function catalog whereas it gives an empty String for procedure and all other database objects.
	 * (this can be seen in org/apache/derby.impl/jdbc/metadata.properties source code, which is not available through Maven but can be downloaded
	 * from Apache Derby website)
	 */
	@Override
	protected ProcedureMetadata convertToFunctionToMetadata(ResultSet resultSet) {
		ProcedureMetadata result = new ProcedureMetadata(
				// fix the null catalog returned by Derby to be alignes with other database objects
				"",
				FunctionMetaDataPseudoTable.INSTANCE.schema.giveValue(resultSet),
				FunctionMetaDataPseudoTable.INSTANCE.name.giveValue(resultSet)
		);
		FunctionMetaDataPseudoTable.INSTANCE.remarks.apply(resultSet, result::setRemarks);
		FunctionMetaDataPseudoTable.INSTANCE.procedureType.apply(resultSet, procedureType -> result.setType(ProcedureType.FUNCTION));
		FunctionMetaDataPseudoTable.INSTANCE.specificName.apply(resultSet, result::setSpecificName);
		return result;
	}
}
