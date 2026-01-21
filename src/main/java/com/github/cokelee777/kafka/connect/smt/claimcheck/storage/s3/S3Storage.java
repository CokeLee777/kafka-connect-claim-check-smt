package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.StorageType;
import com.github.cokelee777.kafka.connect.smt.utils.ConfigUtils;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
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

  private S3Client s3Client;
  private String bucketName;
  private String pathPrefix;

  public S3Storage() {}

  public S3Storage(S3Client s3Client) {
    this.s3Client = s3Client;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getPathPrefix() {
    return pathPrefix;
  }

  @Override
  public String type() {
    return StorageType.S3.type();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(Config.DEFINITION, configs);

    this.bucketName = config.getString(Config.BUCKET_NAME);
    this.pathPrefix = ConfigUtils.normalizePathPrefix(config.getString(Config.PATH_PREFIX));

    if (this.s3Client == null) {
      S3ClientFactory factory = new S3ClientFactory(config);
      this.s3Client = factory.create();
    }
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
