package com.sxt.transformer.mr.active2;

import com.sxt.common.GlobalConstants;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.dim.base.BaseDimension;
import com.sxt.transformer.model.value.BaseStatsValueWritable;
import com.sxt.transformer.model.value.reduce.MapWritableValue;
import com.sxt.transformer.mr.IOutputCollector;
import com.sxt.transformer.service.IDimensionConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author qiong.peng
 * @Date 2019/10/9
 */
public class BrowserActive2UserCollector implements IOutputCollector {

    /**
     *
     INSERT INTO `stats_device_browser`(
     `platform_dimension_id`,
     `date_dimension_id`,
     `browser_dimension_id`,
     `active_users`,
     `created`
     ) VALUES(?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `active_users` = ?
     */
    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue result = (MapWritableValue) value;

        IntWritable activeUser = (IntWritable) result.getValue().get(new IntWritable(-1));

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
        pstmt.setInt(++i, activeUser.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, activeUser.get());

        pstmt.addBatch();
    }
}
