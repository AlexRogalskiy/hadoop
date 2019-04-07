package com.hadoopbook.pig;

import org.apache.pig.PrimitiveEvalFunc;

//cc Trim Funkcja UDF z rodziny EvalFunc usuwająca początkowe i końcowe spacje z wartości typu chararray
//vv Trim
public class Trim extends PrimitiveEvalFunc<String, String> {
  @Override
  public String exec(String input) {
    return input.trim();
  }
}
// ^^ Trim