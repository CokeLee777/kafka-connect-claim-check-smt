package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

public sealed interface CloseableClaimCheckStorage extends ClaimCheckStorage, AutoCloseable
    permits S3Storage {}
