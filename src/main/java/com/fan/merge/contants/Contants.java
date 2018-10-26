package com.fan.merge.contants;

public class Contants {

    public static final String FILE_PREFIX = "combine";
    public static final double COMBINE_COEFFICIENT = 0.5;



    public  static  enum FileType {
        PARQUET("parquet", 1), ORC("orc", 2);
        private String type;
        private int value;

        private FileType(String type, int value) {
            this.type = type;
            this.value = value;
        }
    }


}
