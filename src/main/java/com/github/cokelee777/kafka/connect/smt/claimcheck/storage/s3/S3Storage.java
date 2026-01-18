package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.StorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.retry.RetryStrategyFactory;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * A {@link ClaimCheckStorage} implementation that stores message payloads in Amazon S3.
 *
 * <p>This class handles the configuration of the S3 client and the process of uploading data to a
 * specified S3 bucket. It supports custom endpoint configurations for testing purposes (e.g., with
 * LocalStack) and includes a configurable retry mechanism for uploads.
 */
public class S3Storage implements ClaimCheckStorage {

  /** The S3 bucket name where the message payloads will be stored. */
  public static final String CONFIG_BUCKET_NAME = "storage.s3.bucket.name";

  /** The AWS region for the S3 bucket. */
  public static final String CONFIG_REGION = "storage.s3.region";

  /** A prefix to be added to the S3 object key. */
  public static final String CONFIG_PATH_PREFIX = "storage.s3.path.prefix";

  /** An optional endpoint override for the S3 client, primarily for testing (e.g., LocalStack). */
  public static final String CONFIG_ENDPOINT_OVERRIDE = "storage.s3.endpoint.override";

  /** The maximum number of retries for S3 upload operations upon failure. */
  public static final String CONFIG_RETRY_MAX = "storage.s3.retry.max";

  /** The initial backoff duration in milliseconds between retry attempts. */
  public static final String CONFIG_RETRY_BACKOFF_MS = "storage.s3.retry.backoff.ms";

  /** The maximum backoff duration in milliseconds between retry attempts. */
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

  private RetryStrategyFactory<StandardRetryStrategy> retryStrategyFactory =
      new S3RetryConfigAdapter();

  private String bucketName;
  private String region;
  private String pathPrefix;
  private String endpointOverride;
  private int retryMax;
  private long retryBackoffMs;
  private long retryMaxBackoffMs;

  private S3Client s3Client;

  /**
   * Default constructor. The S3 client will be created and configured based on the properties
   * passed to the {@link #configure(Map)} method.
   */
  public S3Storage() {}

  /**
   * Constructor for testing purposes, allowing injection of a mocked or pre-configured S3 client.
   *
   * @param s3Client The S3 client to use for storage operations.
   * @param retryStrategyFactory A factory to create the retry strategy.
   */
  public S3Storage(
      S3Client s3Client, RetryStrategyFactory<StandardRetryStrategy> retryStrategyFactory) {
    this.s3Client = s3Client;
    this.retryStrategyFactory = retryStrategyFactory;
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
  public String type() {
    return StorageType.S3.type();
  }

  /**
   * Configures the S3 storage backend.
   *
   * <p>This method initializes the S3 client based on the provided configuration properties. If an
   * S3 client has already been injected (e.g., for testing), this method will not create a new one.
   *
   * @param configs The configuration properties for the S3 storage.
   */
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

    if (this.s3Client != null) {
      return;
    }

    RetryConfig retryConfig =
        new RetryConfig(
            // maxAttempts = initial attempt (1) + retry count
            retryMax + 1,
            Duration.ofMillis(this.retryBackoffMs),
            Duration.ofMillis(this.retryMaxBackoffMs));
    StandardRetryStrategy retryStrategy = this.retryStrategyFactory.create(retryConfig);

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

  /**
   * Uploads the given payload to the configured S3 bucket.
   *
   * <p>A unique key is generated for the object, and it is stored under the configured path prefix.
   *
   * @param payload The byte array payload to be stored.
   * @return The S3 URI of the stored object (e.g., "s3://bucket-name/path-prefix/uuid").
   * @throws IllegalStateException if the S3 client is not initialized.
   * @throws RuntimeException if the upload to S3 fails.
   */
  @Override
  public String store(byte[] payload) {
    if (this.s3Client == null) {
      throw new IllegalStateException("S3Client is not initialized. Call configure() first.");
    }

    String key = this.pathPrefix + "/" + UUID.randomUUID();
    try {
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(this.bucketName).key(key).build();
      this.s3Client.putObject(putObjectRequest, RequestBody.fromBytes(payload));

      return "s3://" + this.bucketName + "/" + key;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to upload to S3. Bucket: " + this.bucketName + ", Key: " + key, e);
    }
  }

  /** Closes the underlying S3 client, releasing any resources. */
  @Override
  public void close() {
    if (this.s3Client != null) {
      s3Client.close();
    }
  }
}
