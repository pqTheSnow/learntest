package com.sxt.transformer.mr.active;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import com.sxt.common.DateEnum;
import com.sxt.common.EventLogConstants;
import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsCommonDimension;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.dim.base.BrowserDimension;
import com.sxt.transformer.model.dim.base.DateDimension;
import com.sxt.transformer.model.dim.base.KpiDimension;
import com.sxt.transformer.model.dim.base.PlatformDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;

public class ActiveUserMapper  extends TableMapper<StatsUserDimension, TimeOutputValue>{
	
	byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
	
	StatsUserDimension userDimension = new StatsUserDimension();
	
	TimeOutputValue outputValue = new TimeOutputValue();
	//用户模块
	KpiDimension kpiUserDimension =  new KpiDimension(KpiType.ACTIVE_USER.name);
	//浏览器模块
	KpiDimension kpiBrowserDimension =  new KpiDimension(KpiType.BROWSER_ACTIVE_USER.name);

	@Override
	protected void map(ImmutableBytesWritable key, Result value,	Context context)
					throws IOException, InterruptedException {
		
		String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
		String s_time = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
		String plName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
		
		long timestap = Long.valueOf(s_time);
		outputValue.setId(uuid);
		outputValue.setTime(timestap);
		//构建时间维度..
		DateDimension dateDimension = DateDimension.buildDate(timestap, DateEnum.DAY);
		
		//构建平台维度
		List<PlatformDimension> plList = PlatformDimension.buildList(plName);
		
		//构建浏览器维度...
		String b_name = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
		String b_version = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
		
	    List<BrowserDimension> browserList = BrowserDimension.buildList(b_name, b_version);
		
	    BrowserDimension defaultBrowser = new BrowserDimension("", "");
	    for(PlatformDimension pl: plList){
	    	//用户信息模块的输出
	    	userDimension.setBrowser(defaultBrowser);
	    	StatsCommonDimension statsCommon = userDimension.getStatsCommon();
	    	
	    	statsCommon.setDate(dateDimension);
	    	statsCommon.setPlatform(pl);
	    	statsCommon.setKpi(kpiUserDimension);
	    	
	    	context.write(userDimension, outputValue);
	    	
	    	//浏览器信息模块的输出
	    	for(BrowserDimension br:browserList){
	    		statsCommon.setKpi(kpiBrowserDimension);
	    		userDimension.setBrowser(br);
	    	 	context.write(userDimension, outputValue);
	    	}	    	
	    }

	}
  
}
