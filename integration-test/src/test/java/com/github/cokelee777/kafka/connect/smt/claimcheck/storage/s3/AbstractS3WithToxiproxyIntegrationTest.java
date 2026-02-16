package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.io.IOException;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

abstract class AbstractS3WithToxiproxyIntegrationTest extends AbstractS3IntegrationTest {

  private static final int S3_INTERNAL_PORT = 4566;

  protected static final ToxiproxyContainer toxiproxy =
      new ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
          .withNetwork(network)
          .dependsOn(localstack);

  protected static Proxy s3Proxy;

  static {
    toxiproxy.start();
  }

  @BeforeAll
  static void initializeToxiproxy() throws IOException {
    ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
    String proxyName = "s3-proxy";

    try {
      s3Proxy = client.getProxy(proxyName);
    } catch (IOException e) {
      s3Proxy =
          client.createProxy(
              proxyName, "0.0.0.0:8666", LOCALSTACK_NETWORK_ALIAS + ":" + S3_INTERNAL_PORT);
    }

    for (var toxic : s3Proxy.toxics().getAll()) {
      toxic.remove();
    }
  }

  protected void enableToxiproxy() throws IOException {
    if (!s3Proxy.isEnabled()) {
      s3Proxy.enable();
    }
  }

  protected void clearAllToxics() throws IOException {
    if (s3Proxy != null) {
      for (var toxic : s3Proxy.toxics().getAll()) {
        toxic.remove();
      }
    }
  }

  protected String getProxiedEndpoint() {
    return "http://" + toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(8666);
  }
}
