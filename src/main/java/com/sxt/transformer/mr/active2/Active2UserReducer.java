package com.sxt.transformer.mr.active2;

import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import com.sxt.transformer.model.value.reduce.MapWritableValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author qiong.peng
 * @Date 2019/10/9
 */
public class Active2UserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    Set<String> unique = new HashSet<>();

    private MapWritableValue mapWritableValue = new MapWritableValue();
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        unique.clear();

        for (TimeOutputValue v : values) {
            unique.add(v.getId());
        }

        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1), new IntWritable(unique.size()));
        mapWritableValue.setValue(mapWritable);
        mapWritableValue.setKpi(KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName()));

        context.write(key, mapWritableValue);
    }
}
