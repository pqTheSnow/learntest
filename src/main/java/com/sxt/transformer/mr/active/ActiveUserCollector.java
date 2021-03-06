package com.sxt.transformer.mr.active;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import com.sxt.common.GlobalConstants;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.dim.base.BaseDimension;
import com.sxt.transformer.model.value.BaseStatsValueWritable;
import com.sxt.transformer.model.value.reduce.MapWritableValue;
import com.sxt.transformer.mr.IOutputCollector;
import com.sxt.transformer.service.IDimensionConverter;

public class ActiveUserCollector implements IOutputCollector {


	/**
	 *   INSERT INTO `stats_user`(
		    `platform_dimension_id`,
		    `date_dimension_id`,
		    `active_users`,
		    `created`)
		  VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `active_users` = ?
	 */
	@Override
	public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt,
			IDimensionConverter converter) throws SQLException, IOException {
		StatsUserDimension userDimension = (StatsUserDimension) key;
		
		
		MapWritableValue mapWritableValue = (MapWritableValue)value;
		MapWritable mapWritable = mapWritableValue.getValue();
		IntWritable intWritable = (IntWritable)mapWritable.get(new IntWritable(-1));
		int activeUsers = intWritable.get();
	
		
		pstmt.setInt(1, converter.getDimensionIdByValue(userDimension.getStatsCommon().getPlatform()));
		pstmt.setInt(2, converter.getDimensionIdByValue(userDimension.getStatsCommon().getDate()));
		pstmt.setInt(3, activeUsers);
		pstmt.setString(4, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
		pstmt.setInt(5,activeUsers);
		
		pstmt.addBatch();

	}

}
