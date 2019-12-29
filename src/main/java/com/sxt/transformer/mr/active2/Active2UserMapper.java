package com.sxt.transformer.mr.active2;

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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author qiong.peng
 * @Date 2019/10/9
 */
public class Active2UserMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {

    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);

    private StatsUserDimension statsUserDimension = new StatsUserDimension();

    private TimeOutputValue timeOutputValue = new TimeOutputValue();

    private KpiDimension activeUserKpiDimension = new KpiDimension(KpiType.ACTIVE_USER.name);
    private KpiDimension activeBrowserKpiDimension = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String uuid = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));

        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));

        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));

        String browser = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));

        StatsCommonDimension statsCommonDimension = statsUserDimension.getStatsCommon();

        // 时间维度
        long time = Long.valueOf(serverTime);
        DateDimension dateDimension = DateDimension.buildDate(time, DateEnum.DAY);

        statsCommonDimension.setDate(dateDimension);
        timeOutputValue.setTime(time);
        timeOutputValue.setId(uuid);

        // 平台维度
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        // 浏览器维度
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browser, browserVersion);

        BrowserDimension emptyBrowser = new BrowserDimension("", "");

        for (PlatformDimension platformDimension :
                platformDimensions) {
            statsUserDimension.setBrowser(emptyBrowser);
            statsCommonDimension.setKpi(activeUserKpiDimension);
            statsCommonDimension.setPlatform(platformDimension);

            context.write(statsUserDimension, timeOutputValue);

            for (BrowserDimension browserDimension : browserDimensions) {
                statsUserDimension.setBrowser(browserDimension);
                statsCommonDimension.setKpi(activeBrowserKpiDimension);
                context.write(statsUserDimension, timeOutputValue);
            }
        }
    }
}
