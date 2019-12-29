//package com.sxt.transformer.hive;
//
//
//import java.io.IOException;
//
//import com.sxt.transformer.model.dim.base.PlatformDimension;
//import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//
//import com.sxt.common.DateEnum;
//import com.sxt.transformer.model.dim.base.DateDimension;
//import com.sxt.transformer.service.IDimensionConverter;
//import com.sxt.transformer.service.impl.DimensionConverterImpl;
//import com.sxt.util.TimeUtil;
//
///**
// * 操作日期dimension 相关的udf
// *
// * @author root
// *
// */
//public class PlatformDimensionUDF extends UDF {
//    private IDimensionConverter converter = new DimensionConverterImpl();
//
//
//
//    /**
//     * 根据给定的platform返回id
//     *
//     * @param platform
//     * @return
//     */
//    public IntWritable evaluate(Text platform) {
//        System.out.println("platform = [" + platform.toString() + "]");
//        PlatformDimension dimension = new PlatformDimension(platform.toString());
//        try {
//            int id = this.converter.getDimensionIdByValue(dimension);
//            return new IntWritable(id);
//        } catch (IOException e) {
//            throw new RuntimeException("获取platform id异常,错误信息：", e);
//        }
//    }
//
//}
