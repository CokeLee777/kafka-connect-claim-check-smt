package connect.smt.claimcheck.storage;

import connect.smt.claimcheck.storage.S3Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class S3StorageCloseTest {

  @Mock S3Client mockS3Client;

  @Test
  @DisplayName("s3Client가 존재하면 close() 호출 시 내부 클라이언트도 close 되어야 한다")
  void closeWhenClientExists() {
    // Given
    S3Storage storage = new S3Storage(mockS3Client);

    // When
    storage.close();

    // Then
    verify(mockS3Client, times(1)).close();
  }

  @Test
  @DisplayName("s3Client가 null이어도(초기화 실패 등) close() 호출 시 예외가 발생하지 않아야 한다")
  void closeWhenClientIsNull() {
    // Given
    S3Storage storage = new S3Storage();

    // When & Then
    assertDoesNotThrow(storage::close);
  }
}
