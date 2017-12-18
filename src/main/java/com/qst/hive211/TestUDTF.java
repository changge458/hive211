package com.qst.hive211;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 将一行String格式的数据炸开！！！！
 * 
 * @author chang
 *
 */

@Description(name = "udtf_name", value = "_FUNC_(arg1, arg2, ... argN) - A short description for thefunction", extended = "This is more detail about the function, such as syntax,examples.")
public class TestUDTF extends GenericUDTF {
	private PrimitiveObjectInspector stringOI = null;

	/**
	 * This method will be called exactly once per instance. It performs any
	 * custom initialization logic we need. It is also responsible for verifying
	 * the input types and specifying the output types.
	 */
	@Override
	public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
		// 检查参数个数
		if (args.length != 1) {
			throw new UDFArgumentException("The UDTF should take exactly one argument");
		}
		/*
		 * 检查输入格式是否为基本类型中的String
		 */
		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) args[0])
				.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException("The UDTF should take a string as a parameter");
		}

		stringOI = (PrimitiveObjectInspector) args[0];
		/*
		 * 定义输出格式和表别名
		 */
		// 字段名称：alias1，alias2
		// 字段类型：String,int
		List<String> fieldNames = new ArrayList<String>(1);
		List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);
		fieldNames.add("alias1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		// 通过输出格式和名称，返回ObjectInspector对象
		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	/**
	 * 一行调用一次此方法，使用forward来输出
	 */
	@Override
	public void process(Object[] record) throws HiveException {
		/*
		 * 将object类型强转为String
		 */
		final String recStr = (String) stringOI.getPrimitiveJavaObject(record[0]);
		// 输出返回的定义结构String
		String[] arr = recStr.split(" ");
		for (String word : arr) {
			forward(new Object[]{word});
		}
	}

	@Override
	public void close() throws HiveException {
		// Do nothing.
	}
}