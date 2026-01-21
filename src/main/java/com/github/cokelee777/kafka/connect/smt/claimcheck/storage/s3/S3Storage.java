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

  public static final class Config {

    public static final String BUCKET_NAME = "storage.s3.bucket.name";
    public static final String REGION = "storage.s3.region";
    public static final String PATH_PREFIX = "storage.s3.path.prefix";
    public static final String ENDPOINT_OVERRIDE = "storage.s3.endpoint.override";
    public static final String RETRY_MAX = "storage.s3.retry.max";
    public static final String RETRY_BACKOFF_MS = "storage.s3.retry.backoff.ms";
    public static final String RETRY_MAX_BACKOFF_MS = "storage.s3.retry.max.backoff.ms";

    private static final String DEFAULT_REGION = Region.AP_NORTHEAST_2.id();
    private static final String DEFAULT_PATH_PREFIX = "claim-checks";
    private static final int DEFAULT_RETRY_MAX = 3;
    private static final long DEFAULT_RETRY_BACKOFF_MS = 300L;
    private static final long DEFAULT_MAX_BACKOFF_MS = 20_000L;

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                BUCKET_NAME,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "S3 Bucket Name")
            .define(
                REGION,
                ConfigDef.Type.STRING,
                DEFAULT_REGION,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "AWS Region")
            .define(
                PATH_PREFIX,
                ConfigDef.Type.STRING,
                DEFAULT_PATH_PREFIX,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                "Path prefix for stored objects in S3 bucket.")
            .define(
                ENDPOINT_OVERRIDE,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.LOW,
                "S3 Endpoint Override. For testing purposes only (e.g., with LocalStack).")
            .define(
                RETRY_MAX,
                ConfigDef.Type.INT,
                DEFAULT_RETRY_MAX,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                "Maximum number of retries for S3 upload failures.")
            .define(
                RETRY_BACKOFF_MS,
                ConfigDef.Type.LONG,
                DEFAULT_RETRY_BACKOFF_MS,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.LOW,
                "Initial backoff time in milliseconds between S3 upload retries.")
            .define(
                RETRY_MAX_BACKOFF_MS,
                ConfigDef.Type.LONG,
                DEFAULT_MAX_BACKOFF_MS,
                ConfigDef.Range.atLeast(1L),
                ConfigDef.Importance.LOW,
                "Maximum backoff time in milliseconds for S3 upload retries.");

    private Config() {}
  }

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
    SimpleConfig config = new SimpleConfig(Config.DEFINITION, configs);

    this.bucketName = config.getString(Config.BUCKET_NAME);
    this.region = config.getString(Config.REGION);
    this.pathPrefix = ConfigUtils.normalizePathPrefix(config.getString(Config.PATH_PREFIX));
    this.endpointOverride = config.getString(Config.ENDPOINT_OVERRIDE);
    this.retryMax = config.getInt(Config.RETRY_MAX);
    this.retryBackoffMs = config.getLong(Config.RETRY_BACKOFF_MS);
    this.retryMaxBackoffMs = config.getLong(Config.RETRY_MAX_BACKOFF_MS);

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
