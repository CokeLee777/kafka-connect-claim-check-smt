package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig;
import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.net.URI;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/** Factory for creating configured AWS S3 clients. */
public class S3ClientFactory {

  private static final int INITIAL_ATTEMPT = 1;

  private S3ClientFactory() {}

  /**
   * Creates a configured S3 client.
   *
   * @param config the S3 client configuration
   * @return a configured S3Client instance
   */
  public static S3Client create(S3StorageConfig config) {
    S3ClientBuilder builder =
        S3Client.builder()
            .httpClientBuilder(ApacheHttpClient.builder())
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .overrideConfiguration(createOverrideConfiguration(config))
            .region(Region.of(config.getRegion()));

    if (config.getEndpointOverride() != null) {
      // Use static credentials for LocalStack or custom S3-compatible endpoints (e.g., MinIO)
      // These dummy credentials are required by AWS SDK but not validated by LocalStack
      builder
          .endpointOverride(URI.create(config.getEndpointOverride()))
          .forcePathStyle(true)
          .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
    }

    return builder.build();
  }

  private static ClientOverrideConfiguration createOverrideConfiguration(S3StorageConfig config) {
    RetryConfig retryConfig = createRetryConfig(config);
    StandardRetryStrategy retryStrategy = createRetryStrategy(retryConfig);
    return ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build();
  }

  private static RetryConfig createRetryConfig(S3StorageConfig config) {
    int maxAttempts = config.getRetryMax() + INITIAL_ATTEMPT;
    return new RetryConfig(
        maxAttempts,
        Duration.ofMillis(config.getRetryBackoffMs()),
        Duration.ofMillis(config.getRetryMaxBackoffMs()));
  }

  private static StandardRetryStrategy createRetryStrategy(RetryConfig config) {
    BackoffStrategy backoffStrategy =
        BackoffStrategy.exponentialDelay(config.initialBackoff(), config.maxBackoff());

    return StandardRetryStrategy.builder()
        .maxAttempts(config.maxAttempts())
        .backoffStrategy(backoffStrategy)
        .throttlingBackoffStrategy(backoffStrategy)
        .circuitBreakerEnabled(false)
        .build();
  }
}
