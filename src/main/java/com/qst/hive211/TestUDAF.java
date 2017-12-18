package com.qst.hive211;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

@Description(name = "udaf_name", value = "_FUNC_(arg1, arg2, ... argN) - A short description for thefunction", extended = "This is more detail about the function, such as syntax,examples.")
public class TestUDAF extends AbstractGenericUDAFResolver {

	// 参数为 select testudaf(xxx) 中的参数类型
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {

		// 检查参数个数
		if (info.length != 1) {
			throw new UDFArgumentException("The UDTF should take exactly one argument");
		}
		
		// 
		ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);

		/*
		 * 检查输入格式是否为基本类型中的String
		 */
		if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE && 
				((PrimitiveObjectInspector) oi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
			throw new UDFArgumentException("The UDTF should take a string as a parameter");
		}
		
		return new TotalCharNumEvaluator();
	}
	
	/**
	 * AggregationBuffer 用户来存储各阶段的聚合结果
	 * @author chang
	 *
	 */
	
	public static class TotalCharNumEvaluator extends GenericUDAFEvaluator{
		
		PrimitiveObjectInspector inputOI ;
		PrimitiveObjectInspector outputOI ;
		PrimitiveObjectInspector IntegerOI;
		int total = 0;
		
		/**
		 * 初始化阶段，设定各个阶段的输入输出格式
		 */
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] params) throws HiveException {
			
			super.init(m, params);
			
			//map端读取HQL列，输入为string格式
			if(m == Mode.PARTIAL1 || m == Mode.COMPLETE){
				inputOI = (PrimitiveObjectInspector) params[0];
			}
			//其余阶段，输出格式为int格式
			else{
				IntegerOI = (PrimitiveObjectInspector) params[0];
			}
			ObjectInspector outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorOptions.JAVA);
			
			return outputOI;
		}

		/**
		 * 存储各个阶段的聚合结果
		 */
		public static class SumAggregator extends AbstractAggregationBuffer{
			int sum = 0;
			public void add (int num){
				sum+= num;
			}
		}
		
		
		/**
		 * new一个SumAggregator
		 */
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			return new SumAggregator();
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			SumAggregator sumAgg = new SumAggregator();
		}

		/**
		 * 开始处理聚合数据，迭代每条记录，返回其长度
		 */
		@Override
		public void iterate(AggregationBuffer agg, Object[] params) throws HiveException {
			if(params[0] != null){
				SumAggregator myAgg =  (SumAggregator) agg;
				//得到参数的java对象
				Object o1 = inputOI.getPrimitiveJavaObject(params[0]);
				//将对象的string长度发送给sumAgg
				myAgg.add(String.valueOf(o1).length());
			}
			
		}

		/**
		 * map端的停止操作，可能是combiner的停止阶段，所以不能直接return  sum
		 * 需要定义一个成员变量total来进行存储他的数值。
		 */
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			SumAggregator myAgg =  (SumAggregator) agg;
			total += myAgg.sum;
			return total;
		}

		/**
		 * 合并，有两个对象，一个对象是参数中的agg
		 * 另一个对象是new出来的
		 * 将部分和加入到myagg2中取和
		 * 然后在myagg1中将myagg2的和进行取和
		 */
		@Override
		public void merge(AggregationBuffer agg, Object partial) throws HiveException {
			if(partial != null){
				
				SumAggregator myAgg1 =  (SumAggregator) agg;
				Integer partialSum = (Integer)IntegerOI.getPrimitiveJavaObject(partial);
				
				SumAggregator myAgg2 =  new SumAggregator();
				
				myAgg2.add(partialSum);
				myAgg1.add(myAgg2.sum);
			}
			
		}

		
		/**
		 * reduce的收尾阶段
		 */
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			
			SumAggregator myAgg =  (SumAggregator) agg;
			total = myAgg.sum;
			return total;
			
		}
	}
	
	
	
	
	
	
	

}
