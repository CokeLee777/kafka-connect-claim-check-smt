package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.S3StorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client.S3ClientFactory;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public final class S3Storage implements CloseableClaimCheckStorage {

  private S3StorageConfig config;
  private S3Client s3Client;

  public S3Storage() {}

  public S3StorageConfig getConfig() {
    return config;
  }

  @Override
  public String type() {
    return ClaimCheckStorageType.S3.type();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    config = new S3StorageConfig(configs);

    // Allow test injection of s3Client
    if (s3Client == null) {
      s3Client = S3ClientFactory.create(config);
    }
    Objects.requireNonNull(s3Client, "S3Client not configured");
  }

  @Override
  public String store(byte[] payload) {
    String key = generateUniqueKey();
    String bucketName = config.getBucketName();
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(bucketName).key(key).build();
    try {
      s3Client.putObject(putObjectRequest, RequestBody.fromBytes(payload));
      return buildReferenceUrl(key);
    } catch (S3Exception e) {
      throw new RuntimeException(
          "Failed to upload to S3. Bucket: " + bucketName + ", Key: " + key, e);
    }
  }

  private String generateUniqueKey() {
    return config.getPathPrefix() + "/" + UUID.randomUUID();
  }

  private String buildReferenceUrl(String key) {
    return "s3://" + config.getBucketName() + "/" + key;
  }

  @Override
  public byte[] retrieve(String referenceUrl) {
    String key = parseKeyFrom(referenceUrl);
    String bucketName = config.getBucketName();
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(bucketName).key(key).build();
    try (ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest)) {
      return s3Object.readAllBytes();
    } catch (S3Exception | IOException e) {
      throw new RuntimeException(
          "Failed to retrieve from S3. Bucket: " + bucketName + ", Key: " + key, e);
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
    String bucketName = config.getBucketName();
    if (!bucketName.equals(bucketInUrl)) {
      throw new IllegalArgumentException(
          String.format(
              "Bucket in reference URL ('%s') does not match configured bucket ('%s')",
              bucketInUrl, bucketName));
    }

    return path.substring(firstSlash + 1);
  }

  @Override
  public void close() {
    if (s3Client != null) {
      s3Client.close();
    }
  }
}
