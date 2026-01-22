package com.github.cokelee777.kafka.connect.smt.utils;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ConfigUtils 단위 테스트")
class ConfigUtilsTest {

  @Nested
  @DisplayName("normalizePathPrefix 메서드 테스트")
  class NormalizePathPrefix {

    @Test
    @DisplayName("올바른 pathPrefix를 넣으면 원본 pathPrefix가 반환된다.")
    public void normalPath() {
      // Given
      String originalPathPrefix = "dir1/dir2";

      // When
      String normalizedPathPrefix = ConfigUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isEqualTo(originalPathPrefix);
    }

    @Test
    @DisplayName("suffix가 슬래쉬(/)인 pathPrefix는 해당 슬래쉬가 제거된 pathPrefix가 반환된다.")
    public void removeSuffixSlash() {
      // Given
      String originalPathPrefix = "dir1/dir2/";

      // When
      String normalizedPathPrefix = ConfigUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizedPathPrefix).doesNotEndWith("/");
      assertThat(normalizedPathPrefix)
          .isEqualTo(originalPathPrefix.substring(0, originalPathPrefix.length() - 1));
    }

    @Test
    @DisplayName("prefix가 슬래쉬(/)인 pathPrefix는 해당 슬래쉬가 제거된 pathPrefix가 반환된다.")
    public void removePrefixSlash() {
      // Given
      String originalPathPrefix = "/dir1/dir2";

      // When
      String normalizedPathPrefix = ConfigUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizedPathPrefix).doesNotStartWith("/");
      assertThat(normalizedPathPrefix).isEqualTo(originalPathPrefix.substring(1));
    }

    @Test
    @DisplayName("prefix 또는 suffix에 슬래쉬(/)가 두 개 이상 존재한다면, 모든 슬래쉬가 제거된 pathPrefix가 반환된다.")
    public void removeMultipleSlash1() {
      // Given
      String originalPathPrefix = "//dir1/dir2//";

      // When
      String normalizedPathPrefix = ConfigUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizedPathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizedPathPrefix).doesNotStartWith("//");
      assertThat(normalizedPathPrefix).doesNotStartWith("//");
      assertThat(normalizedPathPrefix)
          .isEqualTo(originalPathPrefix.substring(2, originalPathPrefix.length() - 2));
    }

    @Test
    @DisplayName("pathPrefix 중간에 슬래쉬(/)가 두 개 이상 존재한다면, 하나를 제외한 모든 슬래쉬가 제거된 pathPrefix가 반환된다.")
    public void removeMultipleSlash2() {
      // Given
      String originalPathPrefix = "dir1//dir2";

      // When
      String normalizePathPrefix = ConfigUtils.normalizePathPrefix(originalPathPrefix);

      // Then
      assertThat(normalizePathPrefix).isNotEqualTo(originalPathPrefix);
      assertThat(normalizePathPrefix).doesNotContain("//");
      assertThat(normalizePathPrefix.length()).isEqualTo(originalPathPrefix.length() - 1);
      assertThat(normalizePathPrefix)
          .isEqualTo(originalPathPrefix.substring(0, 5) + originalPathPrefix.substring(6));
    }
  }
}
