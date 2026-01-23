package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryStrategyFactory;
import java.net.URI;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class S3ClientFactory {

  private static final int INITIAL_ATTEMPT = 1;

  private final RetryStrategyFactory<StandardRetryStrategy> retryStrategyFactory;
  private final SdkHttpClient httpClient;
  private final AwsCredentialsProvider credentialsProvider;

  public S3ClientFactory() {
    this(
        new S3RetryStrategyFactory(),
        UrlConnectionHttpClient.builder().build(),
        DefaultCredentialsProvider.builder().build());
  }

  public S3ClientFactory(
      RetryStrategyFactory<StandardRetryStrategy> retryStrategyFactory,
      SdkHttpClient httpClient,
      AwsCredentialsProvider credentialsProvider) {
    this.retryStrategyFactory = retryStrategyFactory;
    this.httpClient = httpClient;
    this.credentialsProvider = credentialsProvider;
  }

  public S3Client create(S3ClientConfig config) {
    S3ClientBuilder builder = createBuilder(config);
    configureRegion(builder, config);
    configureEndpoint(builder, config);
    return builder.build();
  }

  private S3ClientBuilder createBuilder(S3ClientConfig config) {
    return S3Client.builder()
        .httpClient(httpClient)
        .credentialsProvider(credentialsProvider)
        .overrideConfiguration(createOverrideConfiguration(config));
  }

  private void configureRegion(S3ClientBuilder builder, S3ClientConfig config) {
    builder.region(Region.of(config.getRegion()));
  }

  private void configureEndpoint(S3ClientBuilder builder, S3ClientConfig config) {
    String endpointOverride = config.getEndpointOverride();
    if (endpointOverride != null) {
      builder.endpointOverride(URI.create(endpointOverride));
      builder.forcePathStyle(true);
    }
  }

  ClientOverrideConfiguration createOverrideConfiguration(S3ClientConfig config) {
    int maxAttempts = config.getRetryMax() + INITIAL_ATTEMPT;
    RetryConfig retryConfig =
        new RetryConfig(
            maxAttempts,
            Duration.ofMillis(config.getRetryBackoffMs()),
            Duration.ofMillis(config.getRetryMaxBackoffMs()));
    StandardRetryStrategy retryStrategy = retryStrategyFactory.create(retryConfig);

    return ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build();
  }
}
