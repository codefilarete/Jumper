package org.codefilarete.jumper.schema;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class MySQLTest {
	
	@Container
	protected static final MySQLContainer<?> mysqldb = new MySQLContainer<>(DockerImageName.parse("mysql:8.0.44"))
			.withUsername("root")
			.withPassword("")
			.withConnectTimeoutSeconds(20);
}
