package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

/** Configuration for creating an S3 client. */
public class S3ClientConfig {

  private final String region;
  private final String endpointOverride;
  private final int retryMax;
  private final long retryBackoffMs;
  private final long retryMaxBackoffMs;

  /**
   * Creates an S3 client configuration.
   *
   * @param region AWS region
   * @param endpointOverride optional endpoint override for testing
   * @param retryMax maximum retry attempts
   * @param retryBackoffMs initial backoff in milliseconds
   * @param retryMaxBackoffMs maximum backoff in milliseconds
   */
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

  /** Returns the AWS region. */
  public String getRegion() {
    return region;
  }

  /** Returns the endpoint override URL, or {@code null} if not set. */
  public String getEndpointOverride() {
    return endpointOverride;
  }

  /** Returns the maximum number of retry attempts. */
  public int getRetryMax() {
    return retryMax;
  }

  /** Returns the initial backoff in milliseconds. */
  public long getRetryBackoffMs() {
    return retryBackoffMs;
  }

  /** Returns the maximum backoff in milliseconds. */
  public long getRetryMaxBackoffMs() {
    return retryMaxBackoffMs;
  }
}
