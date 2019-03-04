package com.uxsino.spark.common;

import java.io.Serializable;

import org.bson.Document;

public abstract class DocumentPairFunction implements org.apache.spark.api.java.function.PairFunction<Document, String, String>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 7915403629446177529L;


}
