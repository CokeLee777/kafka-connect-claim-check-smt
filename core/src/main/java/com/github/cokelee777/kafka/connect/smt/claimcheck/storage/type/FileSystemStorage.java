package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.type;

import com.github.cokelee777.kafka.connect.smt.claimcheck.config.storage.FileSystemStorageConfig;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client.FileSystemClient;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.client.FileSystemClientFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigException;

/**
 * File system based storage backend for the Claim Check pattern.
 *
 * <p>Stores large payloads to a local or network-mounted file system (e.g., NAS, NFS, SMB) and
 * returns a file:// reference URL for retrieval.
 */
public final class FileSystemStorage implements ClaimCheckStorage {

  private FileSystemStorageConfig config;
  private FileSystemClient fileSystemClient;
  private Path resolvedStoragePath;

  public FileSystemStorage() {}

  public FileSystemStorageConfig getConfig() {
    return config;
  }

  @Override
  public String type() {
    return ClaimCheckStorageType.FILESYSTEM.type();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    config = new FileSystemStorageConfig(configs);
    resolvedStoragePath = validateAndResolveStorageDirectory(config.getNormalizedAbsolutePath());

    // Allow test injection of fileSystemClient
    if (fileSystemClient == null) {
      fileSystemClient = FileSystemClientFactory.create(config);
    }
    Objects.requireNonNull(fileSystemClient, "FileSystemClient not configured");
  }

  private Path validateAndResolveStorageDirectory(Path normalizedAbsolutePath) {
    try {
      if (!Files.exists(normalizedAbsolutePath)) {
        Files.createDirectories(normalizedAbsolutePath);
      }

      Path realPath = normalizedAbsolutePath.toRealPath();
      if (!Files.isDirectory(realPath)) {
        throw new ConfigException("Storage path exists but is not a directory: " + realPath);
      }

      if (!Files.isWritable(realPath)) {
        throw new ConfigException("Storage directory is not writable: " + realPath);
      }

      return realPath;
    } catch (IOException e) {
      throw new ConfigException("Failed to create storage directory: " + normalizedAbsolutePath, e);
    }
  }

  @Override
  public String store(byte[] payload) {
    String filename = generateUniqueFilename();
    Path filePath = resolvedStoragePath.resolve(filename);
    try {
      fileSystemClient.write(filePath, payload);
      return buildReferenceUrl(filePath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to write claim check file: " + filePath, e);
    }
  }

  private String generateUniqueFilename() {
    return UUID.randomUUID().toString();
  }

  private String buildReferenceUrl(Path filePath) {
    return "file://" + filePath.toAbsolutePath().normalize();
  }

  @Override
  public byte[] retrieve(String referenceUrl) {
    Path filePath = parsePathFrom(referenceUrl);
    Path realPath = validateAndGetRealPath(filePath);
    try {
      return fileSystemClient.read(realPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read claim check file: " + realPath, e);
    }
  }

  private Path parsePathFrom(String referenceUrl) {
    Objects.requireNonNull(referenceUrl, "referenceUrl must not be null");
    final String prefix = "file://";

    if (!referenceUrl.startsWith(prefix)) {
      throw new IllegalArgumentException("File reference URL must start with 'file://'");
    }

    String pathStr = referenceUrl.substring(prefix.length());
    return Paths.get(pathStr).toAbsolutePath().normalize();
  }

  private Path validateAndGetRealPath(Path filePath) {
    Path realPath;
    try {
      realPath = filePath.toRealPath();
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Claim check file does not exist or cannot be accessed: " + filePath, e);
    }

    if (!realPath.startsWith(resolvedStoragePath)) {
      throw new IllegalArgumentException(
          String.format(
              "Resolved file path '%s' is outside the configured storage path '%s'",
              realPath, resolvedStoragePath));
    }

    if (!Files.isRegularFile(realPath)) {
      throw new IllegalArgumentException("Claim check path is not a regular file: " + realPath);
    }

    return realPath;
  }
}
