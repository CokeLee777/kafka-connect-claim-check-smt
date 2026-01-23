package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class S3ClientConfigFactory {

  private S3ClientConfigFactory() {}

  public static S3ClientConfig create(SimpleConfig config) {
    String region = config.getString(S3Storage.Config.REGION);
    String endpointOverride = config.getString(S3Storage.Config.ENDPOINT_OVERRIDE);
    int retryMax = config.getInt(S3Storage.Config.RETRY_MAX);
    long retryBackoffMs = config.getLong(S3Storage.Config.RETRY_BACKOFF_MS);
    long retryMaxBackoffMs = config.getLong(S3Storage.Config.RETRY_MAX_BACKOFF_MS);

    return new S3ClientConfig(
        region, endpointOverride, retryMax, retryBackoffMs, retryMaxBackoffMs);
  }
}
