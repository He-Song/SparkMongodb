package com.uxsino.spark.common;

public enum JoinType {
                      Inner("inner"), // 内联
                      FullOuter("outer"), // 全联结，并集
                      LeftOuter("leftouter"),// 左联接
                      RightOuter("rightouter"),// 右联接
                      LeftSemi("leftanti");

    private String value;

    public String getValue() {
        return value;
    }

    JoinType(String value) {
        this.value = value;
    }
}
