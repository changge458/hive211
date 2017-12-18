package com.qst.hive211;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="testadd",value="this is an add function",extended="example")
public class Add extends UDF {
	
	public void evaluate() {
		System.out.println("no params");
		System.exit(0);
	}
	
	public int evaluate(int a,int b) {
		return a+b;
	}
	
	public String evaluate(String a, String b) {
		return a+b;
	}
	
	public String evaluate(int a,String b) {
		return a+b;
	}
	
	

}
