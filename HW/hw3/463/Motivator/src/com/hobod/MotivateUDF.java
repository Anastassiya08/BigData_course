package com.hobod;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MotivateUDF extends UDF {

    public int evaluate(int age){
        return 100 - age;
    }
}
