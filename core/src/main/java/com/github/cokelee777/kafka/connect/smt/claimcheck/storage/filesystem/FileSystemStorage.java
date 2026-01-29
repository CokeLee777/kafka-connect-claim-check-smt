package com.github.cokelee777.kafka.connect.smt.claimcheck.storage.filesystem;

import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorage;
import com.github.cokelee777.kafka.connect.smt.claimcheck.storage.ClaimCheckStorageType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * File system based storage backend for the Claim Check pattern.
 *
 * <p>Stores large payloads to a local or network-mounted file system (e.g., NAS, NFS, SMB) and
 * returns a file:// reference URL for retrieval.
 */
public class FileSystemStorage implements ClaimCheckStorage {

  public static final class Config {

    public static final String PATH = "storage.filesystem.path";

    public static final String DEFAULT_PATH = "claim-checks";

    public static final ConfigDef DEFINITION =
        new ConfigDef()
            .define(
                PATH,
                ConfigDef.Type.STRING,
                DEFAULT_PATH,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                "Directory path for storing claim check files. "
                    + "Can be a local path or a network-mounted path (e.g., /claim-checks).");

    private Config() {}
  }

  private Path storagePath;

  public FileSystemStorage() {}

  public Path getStoragePath() {
    return storagePath;
  }

  @Override
  public String type() {
    return ClaimCheckStorageType.FILESYSTEM.type();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(Config.DEFINITION, configs);

    String path = config.getString(Config.PATH);
    this.storagePath = Paths.get(path).toAbsolutePath().normalize();

    ensureStorageDirectoryExists();

    try {
      this.storagePath = this.storagePath.toRealPath();
    } catch (IOException e) {
      throw new ConfigException(
          "Failed to resolve real path for storage directory: " + this.storagePath, e);
    }
  }

  private void ensureStorageDirectoryExists() {
    try {
      if (!Files.exists(this.storagePath)) {
        Files.createDirectories(this.storagePath);
      }

      if (!Files.isDirectory(this.storagePath)) {
        throw new ConfigException(
            "Storage path exists but is not a directory: " + this.storagePath);
      }

      if (!Files.isWritable(this.storagePath)) {
        throw new ConfigException("Storage directory is not writable: " + this.storagePath);
      }
    } catch (IOException e) {
      throw new ConfigException("Failed to create storage directory: " + this.storagePath, e);
    }
  }

  @Override
  public String store(byte[] payload) {
    checkStoragePathInitialized();

    String filename = generateUniqueFilename();
    Path filePath = this.storagePath.resolve(filename);

    try {
      Files.write(filePath, payload);
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
    checkStoragePathInitialized();

    Path filePath = parsePathFrom(referenceUrl);
    Path realPath = validateAndGetRealPath(filePath);

    try {
      return Files.readAllBytes(realPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read claim check file: " + realPath, e);
    }
  }

  private void checkStoragePathInitialized() {
    if (this.storagePath == null) {
      throw new IllegalStateException(
          "FileSystemStorage is not configured. Call configure() first.");
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

    if (!realPath.startsWith(this.storagePath)) {
      throw new IllegalArgumentException(
          String.format(
              "Resolved file path '%s' is outside the configured storage path '%s'",
              realPath, this.storagePath));
    }

    if (!Files.isRegularFile(realPath)) {
      throw new IllegalArgumentException("Claim check path is not a regular file: " + realPath);
    }

    return realPath;
  }

  @Override
  public void close() {
    // No resources to release for file system storage
  }
}
