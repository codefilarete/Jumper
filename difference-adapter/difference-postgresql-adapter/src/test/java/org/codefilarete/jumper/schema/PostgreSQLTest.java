package org.codefilarete.jumper.schema;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class PostgreSQLTest {
	
	@Container
	protected static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>(DockerImageName.parse("postgres:13.6"))
			.withUsername("root")
			.withPassword("root")
			.withConnectTimeoutSeconds(20);
}
