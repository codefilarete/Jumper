package org.codefilarete.jumper.schema;

import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class MariaDBTest {
	
	@Container
	protected static final MariaDBContainer<?> mariadb = new MariaDBContainer<>(DockerImageName.parse("mariadb:10.4"))
			.withUsername("root")
			.withPassword("")
			.withConnectTimeoutSeconds(20);
}
