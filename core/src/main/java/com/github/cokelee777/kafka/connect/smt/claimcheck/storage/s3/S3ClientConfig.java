package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

public class S3ClientConfig {

  private final String region;
  private final String endpointOverride;
  private final int retryMax;
  private final long retryBackoffMs;
  private final long retryMaxBackoffMs;

  public S3ClientConfig(
      String region,
      String endpointOverride,
      int retryMax,
      long retryBackoffMs,
      long retryMaxBackoffMs) {
    this.region = region;
    this.endpointOverride = endpointOverride;
    this.retryMax = retryMax;
    this.retryBackoffMs = retryBackoffMs;
    this.retryMaxBackoffMs = retryMaxBackoffMs;
  }

  public String getRegion() {
    return region;
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
}
