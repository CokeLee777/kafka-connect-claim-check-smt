package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

abstract class AbstractS3IntegrationTest {

  private static final Logger log =
      LoggerFactory.getLogger(AbstractS3IntegrationTest.class.getName());

  protected static final String TOPIC_NAME = "test-topic";
  protected static final String BUCKET_NAME = "test-bucket";
  protected static final String LOCALSTACK_NETWORK_ALIAS = "localstack";

  protected static final Network network = Network.newNetwork();

  protected static final LocalStackContainer localstack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.2.0"))
          .withServices(LocalStackContainer.Service.S3)
          .withNetwork(network)
          .withNetworkAliases(LOCALSTACK_NETWORK_ALIAS);

  protected static S3Client s3Client;

  static {
    localstack.start();
  }

  @BeforeAll
  static void initializeS3() {
    s3Client =
        S3Client.builder()
            .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .region(Region.of(localstack.getRegion()))
            .build();

    try {
      s3Client.headBucket(b -> b.bucket(BUCKET_NAME));
    } catch (NoSuchBucketException e) {
      s3Client.createBucket(b -> b.bucket(BUCKET_NAME));
    } catch (Exception e) {
      log.error("Failed to initialize s3 bucket {}", BUCKET_NAME, e);
    }
  }
}
