package com.csg.airtel.aaa4j.domain.util;

public  final class CommonUtils {
    private CommonUtils() {
    }

    public static Long bytesToMb(Long bytes) {
        if (bytes == null || bytes <= 0) {
            return 0L;
        }
        return bytes / 1_048_576;
    }

}
