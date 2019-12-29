package com.sxt.transformer.mr.active;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import com.sxt.transformer.model.value.reduce.MapWritableValue;

public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
	
	
	@Override
	protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values,
			Context context)
					throws IOException, InterruptedException {
		Set<String> set  = new HashSet<String>();
		
		for(TimeOutputValue outputValue :values){
			set.add(outputValue.getId());
		}
		
		MapWritableValue outputValue = new MapWritableValue();
		//设置mapwritable
		MapWritable mapWritable = new MapWritable();
		mapWritable.put(new IntWritable(-1), new IntWritable(set.size()));
		outputValue.setValue(mapWritable);
		
		//设置kpiType
		String kpiName = key.getStatsCommon().getKpi().getKpiName();

		outputValue.setKpi(KpiType.valueOfName(kpiName));

		
		context.write(key, outputValue);
		
	}
	
}
