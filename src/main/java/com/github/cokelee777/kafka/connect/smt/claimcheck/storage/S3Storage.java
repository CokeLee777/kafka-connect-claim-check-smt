package com.github.cokelee777.kafka.connect.smt.claimcheck.storage;

import com.github.cokelee777.kafka.connect.smt.utils.ConfigUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

public class S3Storage implements ClaimCheckStorage {

  public static final String CONFIG_BUCKET_NAME = "storage.s3.bucket.name";
  public static final String CONFIG_REGION = "storage.s3.region";
  public static final String CONFIG_S3_PATH_PREFIX = "storage.s3.path.prefix";
  public static final String CONFIG_ENDPOINT_OVERRIDE = "storage.s3.endpoint.override";
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
              CONFIG_S3_PATH_PREFIX,
              ConfigDef.Type.STRING,
              "claim-checks/",
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
              "S3 Endpoint Override (For Testing)");

  private String bucketName;
  private String region;
  private String pathPrefix;
  private String endpointOverride;

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

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

    this.bucketName = ConfigUtils.getRequiredString(config, CONFIG_BUCKET_NAME);
    this.region = ConfigUtils.getOptionalString(config, CONFIG_REGION);
    this.pathPrefix = ConfigUtils.getOptionalString(config, CONFIG_S3_PATH_PREFIX);
    this.endpointOverride = ConfigUtils.getOptionalString(config, CONFIG_ENDPOINT_OVERRIDE);

    S3ClientBuilder builder =
        S3Client.builder().credentialsProvider(DefaultCredentialsProvider.builder().build());

    builder.region(Region.of(this.region));

    if (this.endpointOverride != null) {
      builder.endpointOverride(URI.create(this.endpointOverride));
      builder.forcePathStyle(true);
    }

    this.s3Client = builder.build();
  }

  @Override
  public String store(byte[] data) {
    if (this.s3Client == null) {
      throw new IllegalStateException("S3Client is not initialized. Call configure() first.");
    }

    String key = this.pathPrefix + UUID.randomUUID();
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
