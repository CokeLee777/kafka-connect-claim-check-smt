package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryStrategyFactory;
import java.net.URI;
import java.time.Duration;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class S3ClientFactory {

  private final SimpleConfig config;
  private final RetryStrategyFactory<StandardRetryStrategy> retryStrategyFactory;

  public S3ClientFactory(SimpleConfig config) {
    this.config = config;
    this.retryStrategyFactory = new S3RetryStrategyFactory();
  }

  public S3Client create() {
    S3ClientBuilder builder =
        S3Client.builder()
            .httpClient(UrlConnectionHttpClient.builder().build())
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .overrideConfiguration(createOverrideConfiguration());

    String region = config.getString(S3Storage.Config.REGION);
    builder.region(Region.of(region));

    String endpointOverride = config.getString(S3Storage.Config.ENDPOINT_OVERRIDE);
    if (endpointOverride != null) {
      builder.endpointOverride(URI.create(endpointOverride));
      builder.forcePathStyle(true);
    }

    return builder.build();
  }

  public ClientOverrideConfiguration createOverrideConfiguration() {
    int retryMax = config.getInt(S3Storage.Config.RETRY_MAX);
    long retryBackoffMs = config.getLong(S3Storage.Config.RETRY_BACKOFF_MS);
    long retryMaxBackoffMs = config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS);

    RetryConfig retryConfig =
        new RetryConfig(
            // maxAttempts = initial attempt (1) + retry count
            retryMax + 1, Duration.ofMillis(retryBackoffMs), Duration.ofMillis(retryMaxBackoffMs));
    StandardRetryStrategy retryStrategy = retryStrategyFactory.create(retryConfig);

    return ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build();
  }
}
