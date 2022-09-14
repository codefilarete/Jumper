package org.codefilarete.jumper.ddl.dsl;

import java.sql.SQLException;

import org.codefilarete.jumper.ChangeSet;
import org.codefilarete.jumper.ChangeSetRunner;
import org.codefilarete.jumper.DataSourceConnectionProvider;
import org.codefilarete.tool.trace.Chrono;
import org.mariadb.jdbc.MariaDbDataSource;

public class ErpTest {


//	@Test
	public static void main(String[] args) throws SQLException {
//	void apiUsage() throws SQLException {
		MariaDbDataSource dataSource = new MariaDbDataSource("jdbc:mariadb://localhost:" + 3307 + "/" + "integrationTests");
		dataSource.setUser("root");
		dataSource.setPassword("root");
		
		Chrono c = new Chrono();
		ChangeSetRunner init_schema = ChangeSetRunner.forJdbcStorage(new DataSourceConnectionProvider(dataSource),
				new ChangeSet("BADRE-125-table", false).addChanges(
						DDLEase.createTable("Visit")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("key", "varchar(10)").notNull().uniqueConstraint("UK_visit_key")
								.addColumn("visitDate", "DATE")
								.addColumn("startTime", "TIME")
								.addColumn("endTime", "TIME")
								.addColumn("warning", "BOOLEAN").defaultValue("false").notNull()
								.addColumn("caregiverId", "BIGINT").notNull()
								.addColumn("patientId", "BIGINT").notNull()
								.addColumn("status", "VARCHAR(50)")
								.addColumn("type", "VARCHAR(50)")
								.build(),
						DDLEase.createTable("Patient")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("key", "varchar(10)").notNull().uniqueConstraint("UK_patient_key")
								.addColumn("firstname", "VARCHAR(70)")
								.addColumn("lastname", "VARCHAR(100)")
								.addColumn("gender", "VARCHAR(10)")
								.addColumn("address", "VARCHAR(300)")
								.addColumn("contactPhone", "VARCHAR(20)")
								.addColumn("patientPhone", "VARCHAR(20)")
								.addColumn("birthdate", "Date")
								.addColumn("healthCareId", "VARCHAR(15)")
								.addColumn("email", "VARCHAR(70)")
								.addColumn("nextMedicalAppointment", "DATE")
								.addColumn("instruction", "VARCHAR(4000)")
								.build(),
						DDLEase.createTable("Caregiver")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("key", "VARCHAR(8)")
								.addColumn("firstname", "VARCHAR(70)")
								.addColumn("lastname", "VARCHAR(100)")
								.build(),
						DDLEase.createTable("ThirdParty")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("firstname", "VARCHAR(70)")
								.addColumn("lastname", "VARCHAR(100)")
								.addColumn("title", "VARCHAR(10)")
								.addColumn("speciality", "VARCHAR(100)")
								.build(),
						DDLEase.createTable("Prescription")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("prescriberId", "BIGINT").notNull()
								.addColumn("patientId", "BIGINT").notNull()
								.addColumn("domainId", "BIGINT").notNull()
								.addColumn("startDate", "DATE")
								.addColumn("endDate", "DATE")
								.build(),
						DDLEase.createTable("Domain")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("code", "VARCHAR(10)")
								.build(),
						DDLEase.createTable("Treatment")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("code", "VARCHAR(10)")
								.addColumn("domainId", "BIGINT").notNull()
								.build(),
						DDLEase.createTable("Prescription_Treatment")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("prescriptionId", "BIGINT").notNull()
								.addColumn("treatmentId", "BIGINT").notNull()
								.build(),
						DDLEase.createTable("Visit_Prescription")
								.addColumn("id", "BIGINT").autoIncrement().primaryKey()
								.addColumn("prescriptionId", "BIGINT").notNull()
								.addColumn("visitId", "BIGINT").notNull()
								.build(),
						
						DDLEase.createForeignKey("patientIdFK", "Visit")
								.addSourceColumn("patientId")
								.targetTable("Patient").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("caregiverIdFK", "Visit")
								.addSourceColumn("caregiverId")
								.targetTable("Caregiver").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("prescriptionPatientIdFK", "Prescription")
								.addSourceColumn("patientId")
								.targetTable("Patient").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("prescribertIdFK", "Prescription")
								.addSourceColumn("prescriberId")
								.targetTable("ThirdParty").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("FKmxn01wuun6ld9simywsbajaec", "Prescription")
								.addSourceColumn("domainId")
								.targetTable("Domain").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("FK3369ijr98gjqf93f5nxguhjpw", "Treatment")
								.addSourceColumn("domainId")
								.targetTable("Domain").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("FK9xtnsfbuhuk176e72bnqjqq0s", "Visit_Prescription")
								.addSourceColumn("visitId")
								.targetTable("Visit").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("FKhtfcji3ssawa79709phv3aju1", "Visit_Prescription")
								.addSourceColumn("prescriptionId")
								.targetTable("Prescription").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("FKp4bpn02bpi39ucl7fn1jaac6y", "Prescription_Treatment")
								.addSourceColumn("prescriptionId")
								.targetTable("Prescription").addTargetColumn("id")
								.build(),
						DDLEase.createForeignKey("FKdmwbhv83vrp4u7qn587elxefs", "Prescription_Treatment")
								.addSourceColumn("treatmentId")
								.targetTable("Treatment").addTargetColumn("id")
								.build(),
						
						DDLEase.createIndex("a", "Prescription_Treatment")
								.addColumn("prescriptionId")
								.addColumn("treatmentId")
								.unique()
								.build(),
						DDLEase.createIndex("b", "Visit_Prescription")
								.addColumn("visitId")
								.addColumn("prescriptionId")
								.unique()
								.build()
						));
		System.out.println(c);
		c.start();
		init_schema.processUpdate();
		System.out.println(c);
	}
}
