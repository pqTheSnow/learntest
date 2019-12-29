package com.cmccstormjk02.web;

import com.cmccstormjk02.cmcc.hbase.dao.HBaseDAO;
import com.cmccstormjk02.cmcc.hbase.dao.impl.HBaseDAOImp;
import com.cmccstormjk02.tools.DateFmt;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.Gson;

/**
 * @author qiong.peng
 * @Date 2019/10/18
 */
@Controller
public class CMCCController {

    private HBaseDAO hBaseDAO = new HBaseDAOImp();
    Gson gson = new Gson();

    @RequestMapping("/")
    @ResponseBody
    public String index(String cell_num) throws Exception {
//        String cell_num = request.getParameter("cell_num");

        // 取当前
        String today = DateFmt.getCountDate(null, DateFmt.date_short);

        List list = new ArrayList();

        // 去当前时间的值
        Result rs = hBaseDAO.getOneRowAndMultiColumn("cell_monitor_table", cell_num + "_" + today,
                new String[]{DateFmt.getCountDate(null, DateFmt.date_minute)});
        for (Cell cell : rs.rawCells()) {
            System.out.println("RowName:" + new String(CellUtil.cloneRow(cell)) + " ");
            System.out.println("Timetamp:" + cell.getTimestamp() + " ");
            System.out.println("column Family:" + new String(CellUtil.cloneFamily(cell)) + " ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");
            list.add(new String(CellUtil.cloneValue(cell)));
        }
        return gson.toJson(list);
    }
}
