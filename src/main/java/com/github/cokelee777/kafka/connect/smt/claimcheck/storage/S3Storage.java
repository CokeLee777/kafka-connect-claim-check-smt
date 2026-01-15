package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import com.github.cokelee777.kafka.connect.smt.utils.ConfigUtils;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Storage implements ClaimCheckStorage {

  public static final String CONFIG_BUCKET_NAME = "storage.s3.bucket.name";
  public static final String CONFIG_REGION = "storage.s3.region";
  public static final String CONFIG_PATH_PREFIX = "storage.s3.path.prefix";
  public static final String CONFIG_ENDPOINT_OVERRIDE = "storage.s3.endpoint.override";
  public static final String CONFIG_RETRY_MAX = "storage.s3.retry.max";
  public static final String CONFIG_RETRY_BACKOFF_MS = "storage.s3.retry.backoff.ms";
  public static final String CONFIG_RETRY_MAX_BACKOFF_MS = "storage.s3.retry.max.backoff.ms";
  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              CONFIG_BUCKET_NAME,
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH,
              "S3 Bucket Name")
          .define(
              CONFIG_REGION,
              ConfigDef.Type.STRING,
              "ap-northeast-2",
              ConfigDef.Importance.MEDIUM,
              "AWS Region")
          .define(
              CONFIG_PATH_PREFIX,
              ConfigDef.Type.STRING,
              "claim-checks",
              ConfigDef.Importance.LOW,
              "Path prefix for stored objects in S3 bucket.")
          .define(
              CONFIG_ENDPOINT_OVERRIDE,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.LOW,
              "S3 Endpoint Override. For testing purposes only (e.g., with LocalStack).",
              "Testing",
              1,
              ConfigDef.Width.MEDIUM,
              "S3 Endpoint Override (For Testing)")
          .define(
              CONFIG_RETRY_MAX,
              ConfigDef.Type.INT,
              3,
              ConfigDef.Range.atLeast(0),
              ConfigDef.Importance.MEDIUM,
              "Maximum number of retries for S3 upload failures.")
          .define(
              CONFIG_RETRY_BACKOFF_MS,
              ConfigDef.Type.LONG,
              300L,
              ConfigDef.Range.atLeast(1L),
              ConfigDef.Importance.LOW,
              "Initial backoff time in milliseconds between S3 upload retries.")
          .define(
              CONFIG_RETRY_MAX_BACKOFF_MS,
              ConfigDef.Type.LONG,
              20_000L,
              ConfigDef.Range.atLeast(1L),
              ConfigDef.Importance.LOW,
              "Maximum backoff time in milliseconds for S3 upload retries.");
  ;

  private String bucketName;
  private String region;
  private String pathPrefix;
  private String endpointOverride;
  private int retryMax;
  private long retryBackoffMs;
  private long retryMaxBackoffMs;

  private S3Client s3Client;

  public S3Storage() {}

  public S3Storage(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getRegion() {
    return region;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }

  public String getEndpointOverride() {
    return endpointOverride;
  }

  public int getRetryMax() {
    return retryMax;
  }

  public long getRetryBackoffMs() {
    return retryBackoffMs;
  }

  public long getRetryMaxBackoffMs() {
    return retryMaxBackoffMs;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

    this.bucketName = ConfigUtils.getRequiredString(config, CONFIG_BUCKET_NAME);
    this.region = config.getString(CONFIG_REGION);
    this.pathPrefix = ConfigUtils.normalizePathPrefix(config.getString(CONFIG_PATH_PREFIX));
    this.endpointOverride = ConfigUtils.getOptionalString(config, CONFIG_ENDPOINT_OVERRIDE);
    this.retryMax = config.getInt(CONFIG_RETRY_MAX);
    this.retryBackoffMs = config.getLong(CONFIG_RETRY_BACKOFF_MS);
    this.retryMaxBackoffMs = config.getLong(CONFIG_RETRY_MAX_BACKOFF_MS);

    StandardRetryStrategy retryStrategy = initRetryStrategy();

    S3ClientBuilder builder =
        S3Client.builder()
            .httpClient(UrlConnectionHttpClient.builder().build())
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .overrideConfiguration(
                ClientOverrideConfiguration.builder().retryStrategy(retryStrategy).build());

    builder.region(Region.of(this.region));

    if (this.endpointOverride != null) {
      builder.endpointOverride(URI.create(this.endpointOverride));
      builder.forcePathStyle(true);
    }

    this.s3Client = builder.build();
  }

  private StandardRetryStrategy initRetryStrategy() {
    BackoffStrategy backoffStrategy =
        BackoffStrategy.exponentialDelay(
            Duration.ofMillis(this.retryBackoffMs), Duration.ofMillis(this.retryMaxBackoffMs));

    return StandardRetryStrategy.builder()
        .maxAttempts(this.retryMax + 1)
        .backoffStrategy(backoffStrategy)
        .throttlingBackoffStrategy(backoffStrategy)
        .circuitBreakerEnabled(false)
        .build();
  }

  @Override
  public String store(byte[] data) {
    if (this.s3Client == null) {
      throw new IllegalStateException("S3Client is not initialized. Call configure() first.");
    }

    String key = this.pathPrefix + "/" + UUID.randomUUID();
    try {
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(this.bucketName).key(key).build();
      this.s3Client.putObject(putObjectRequest, RequestBody.fromBytes(data));

      return "s3://" + this.bucketName + "/" + key;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to upload to S3. Bucket: " + this.bucketName + ", Key: " + key, e);
    }
  }

  @Override
  public void close() {
    if (this.s3Client != null) {
      s3Client.close();
    }
  }
}
