package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.common.retry.RetryConfig;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/** Factory for creating configured AWS S3 clients. */
public class S3ClientFactory {

  private static final int INITIAL_ATTEMPT = 1;

  private final SdkHttpClient httpClient;
  private final AwsCredentialsProvider credentialsProvider;

  /** Creates a factory with default AWS SDK components. */
  public S3ClientFactory() {
    this(UrlConnectionHttpClient.builder().build(), DefaultCredentialsProvider.builder().build());
  }

  /**
   * Creates a factory with custom AWS SDK components.
   *
   * @param httpClient HTTP client for S3 requests
   * @param credentialsProvider AWS credentials provider
   */
  public S3ClientFactory(SdkHttpClient httpClient, AwsCredentialsProvider credentialsProvider) {
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
    this.credentialsProvider =
        Objects.requireNonNull(credentialsProvider, "credentialsProvider must not be null");
  }

  /**
   * Creates a configured S3 client.
   *
   * @param config the S3 client configuration
   * @return a configured S3Client instance
   */
  public S3Client create(S3ClientConfig config) {
    S3ClientBuilder builder =
        S3Client.builder()
            .httpClient(httpClient)
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(createOverrideConfiguration(config))
            .region(Region.of(config.region()));

    if (config.endpointOverride() != null) {
      builder.endpointOverride(URI.create(config.endpointOverride())).forcePathStyle(true);
    }

    return builder.build();
  }

  ClientOverrideConfiguration createOverrideConfiguration(S3ClientConfig config) {
    RetryConfig retryConfig = createRetryConfig(config);
    StandardRetryStrategy retryStrategy = createRetryStrategy(retryConfig);
    return ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build();
  }

  private RetryConfig createRetryConfig(S3ClientConfig config) {
    int maxAttempts = config.retryMax() + INITIAL_ATTEMPT;
    return new RetryConfig(
        maxAttempts,
        Duration.ofMillis(config.retryBackoffMs()),
        Duration.ofMillis(config.retryMaxBackoffMs()));
  }

  StandardRetryStrategy createRetryStrategy(RetryConfig config) {
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
