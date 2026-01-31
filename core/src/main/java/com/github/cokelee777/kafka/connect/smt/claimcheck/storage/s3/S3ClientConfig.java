package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.s3;

/**
 * Configuration for creating an S3 client.
 *
 * @param region AWS region
 * @param endpointOverride optional endpoint override for testing
 * @param retryMax maximum retry attempts
 * @param retryBackoffMs initial backoff in milliseconds
 * @param retryMaxBackoffMs maximum backoff in milliseconds
 */
public record S3ClientConfig(
    String region,
    String endpointOverride,
    int retryMax,
    long retryBackoffMs,
    long retryMaxBackoffMs) {}
