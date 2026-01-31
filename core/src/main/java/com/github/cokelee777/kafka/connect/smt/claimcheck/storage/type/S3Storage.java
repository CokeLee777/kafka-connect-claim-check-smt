package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3ClientConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3.S3ClientFactory;
import com.github.cokelee777.kafka.connect.smt.common.utils.PathUtils;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public final class S3Storage implements ClaimCheckStorage {

  public static final class Config {

    public static final String BUCKET_NAME = "storage.s3.bucket.name";
    public static final String REGION = "storage.s3.region";
    public static final String PATH_PREFIX = "storage.s3.path.prefix";
    public static final String ENDPOINT_OVERRIDE = "storage.s3.endpoint.override";
    public static final String RETRY_MAX = "storage.s3.retry.max";
    public static final String RETRY_BACKOFF_MS = "storage.s3.retry.backoff.ms";
    public static final String RETRY_MAX_BACKOFF_MS = "storage.s3.retry.max.backoff.ms";

    public static final String DEFAULT_REGION = Region.AP_NORTHEAST_2.id();
    public static final String DEFAULT_PATH_PREFIX = "claim-checks";
    public static final int DEFAULT_RETRY_MAX = 3;
    public static final long DEFAULT_RETRY_BACKOFF_MS = 300L;
    public static final long DEFAULT_MAX_BACKOFF_MS = 20_000L;

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

    public static S3ClientConfig toS3ClientConfig(SimpleConfig config) {
      return new S3ClientConfig(
          config.getString(REGION),
          config.getString(ENDPOINT_OVERRIDE),
          config.getInt(RETRY_MAX),
          config.getLong(RETRY_BACKOFF_MS),
          config.getLong(RETRY_MAX_BACKOFF_MS));
    }

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
    return ClaimCheckStorageType.S3.type();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(Config.DEFINITION, configs);

    this.bucketName = config.getString(Config.BUCKET_NAME);
    this.pathPrefix = PathUtils.normalizePathPrefix(config.getString(Config.PATH_PREFIX));

    if (this.s3Client == null) {
      S3ClientConfig s3ClientConfig = Config.toS3ClientConfig(config);
      S3ClientFactory s3ClientFactory = new S3ClientFactory();
      this.s3Client = s3ClientFactory.create(s3ClientConfig);
    }
  }

  @Override
  public String store(byte[] payload) {
    checkClientInitialized();

    String key = generateUniqueKey();
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(this.bucketName).key(key).build();
    try {
      this.s3Client.putObject(putObjectRequest, RequestBody.fromBytes(payload));
      return buildReferenceUrl(key);
    } catch (S3Exception e) {
      throw new RuntimeException(
          "Failed to upload to S3. Bucket: " + this.bucketName + ", Key: " + key, e);
    }
  }

  private String generateUniqueKey() {
    return this.pathPrefix + "/" + UUID.randomUUID();
  }

  private String buildReferenceUrl(String key) {
    return "s3://" + this.bucketName + "/" + key;
  }

  @Override
  public byte[] retrieve(String referenceUrl) {
    checkClientInitialized();

    String key = parseKeyFrom(referenceUrl);
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(this.bucketName).key(key).build();
    try (ResponseInputStream<GetObjectResponse> s3Object =
        this.s3Client.getObject(getObjectRequest)) {
      return s3Object.readAllBytes();
    } catch (S3Exception | IOException e) {
      throw new RuntimeException(
          "Failed to retrieve from S3. Bucket: " + this.bucketName + ", Key: " + key, e);
    }
  }

  private void checkClientInitialized() {
    if (this.s3Client == null) {
      throw new IllegalStateException("S3Client is not initialized. Call configure() first.");
    }
  }

  @Override
  public void close() {
    if (this.s3Client != null) {
      s3Client.close();
    }
  }

  private String parseKeyFrom(String referenceUrl) {
    Objects.requireNonNull(referenceUrl, "referenceUrl must not be null");
    final String prefix = "s3://";
    if (!referenceUrl.startsWith(prefix)) {
      throw new IllegalArgumentException("S3 reference URL must start with 's3://'");
    }

    String path = referenceUrl.substring(prefix.length());
    int firstSlash = path.indexOf('/');
    if (firstSlash == -1 || firstSlash == 0 || firstSlash == path.length() - 1) {
      throw new IllegalArgumentException("Invalid S3 reference URL: " + referenceUrl);
    }

    String bucketInUrl = path.substring(0, firstSlash);
    if (!this.bucketName.equals(bucketInUrl)) {
      throw new IllegalArgumentException(
          String.format(
              "Bucket in reference URL ('%s') does not match configured bucket ('%s')",
              bucketInUrl, this.bucketName));
    }

    return path.substring(firstSlash + 1);
  }
}
