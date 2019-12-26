package vcraft.HbaseTest.util;




import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import vcraft.HbaseTest.dto.AlarmDto;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
* @ProjectName: msg-take
* @Package: com.saic.utils
* @ClassName: QueryHbase
* @Author: jnfiz
* @Description: ${description}
* @Date: 2019/11/8 9:29
* @Version: 1.0
*/

public class QueryHbase {

    Connection connection = null;
    Table table = null;
    String platCode = null;

    public QueryHbase(String zk, String zkPort, String tableName, String platCode) {
        this.connection = HBaseUtil.getHBaseConnection(zk, zkPort);
        this.platCode = platCode;
        try {
            this.table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void hbaseClose() {
        try {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public MessageBean queryByRowkey(String rowKey) {

        MessageBean messageBean = new MessageBean();
        try {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            byte[] message = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("message"));
            byte[] collectDate = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("collect_date"));
            String gbMessage = Bytes.toString(message);
            String tboxTime = Bytes.toString(collectDate);
            System.out.println(gbMessage);
            messageBean.setGbMessage(gbMessage); 
            System.out.println(tboxTime);
            messageBean.setTboxTime(tboxTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return messageBean;
    }
	
    public Deque<AlarmDto> filterBeginTime(String start,String end) throws IOException {
        //RegexStringComparator 正则
        //SubstringComparator; subString比较器
        //BinaryComparator 二进制比较器
        //and条件
    	Deque<AlarmDto> result = new ArrayDeque<>(4096);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(
        		Bytes.toBytes("info"),
        		Bytes.toBytes("beginTime"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes(start));
        SingleColumnValueFilter endFilter = new SingleColumnValueFilter(
        		Bytes.toBytes("info"),
        		Bytes.toBytes("beginTime"),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                Bytes.toBytes(end));
        filterList.addFilter(startFilter);
        filterList.addFilter(endFilter);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        Table table = connection.getTable(TableName.valueOf("saic:alarm"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        //File output = new File("innerOutput1.log");
        //BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        int cnt = 0;
        while (it.hasNext()){
            Result r = it.next();
            cnt++;
            result.add(new AlarmDto(r));
            //bw.write(new AlarmDto(result).toString());
            
            //HbaseUtils.showResult(result);
        }
        System.out.println("Total:" + cnt);
        //bw.close();
        return result;
    }
    
    public void filter2(String start,String end) throws IOException {
        //RegexStringComparator 正则
        //SubstringComparator; subString比较器
        //BinaryComparator 二进制比较器
        //and条件
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(
        		Bytes.toBytes("info"),
        		Bytes.toBytes("receiveTime"),
                CompareFilter.CompareOp.GREATER_OR_EQUAL,
                Bytes.toBytes(start));
        SingleColumnValueFilter endFilter = new SingleColumnValueFilter(
        		Bytes.toBytes("info"),
        		Bytes.toBytes("receiveTime"),
                CompareFilter.CompareOp.LESS_OR_EQUAL,
                Bytes.toBytes(end));
        filterList.addFilter(startFilter);
        filterList.addFilter(endFilter);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        Table table = connection.getTable(TableName.valueOf("saic:alarm"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        File output = new File("innerOutput2.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        int cnt = 0;
        while (it.hasNext()){
            Result result = it.next();
            cnt++;
            bw.write(new AlarmDto(result).toString());
            
            //HbaseUtils.showResult(result);
        }
        System.out.println("Total:" + cnt);
        bw.close();
    }
    
    public void filter3(String vin) throws IOException {
        //RegexStringComparator 正则
        //SubstringComparator; subString比较器
        //BinaryComparator 二进制比较器
        //and条件
        //FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        SingleColumnValueFilter endFilter = new SingleColumnValueFilter(
        		Bytes.toBytes("info"),
        		Bytes.toBytes("vin"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(vin));
        //filterList.addFilter(endFilter);
        Scan scan = new Scan();
        //PrefixFilter pfilter = new PrefixFilter(Bytes.toBytes(vin));
        scan.setFilter(endFilter);
        Table table = connection.getTable(TableName.valueOf("saic:alarm"));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        File output = new File("innerOutput3.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
        int cnt = 0;
        while (it.hasNext()){
            Result result = it.next();
            cnt++;
            bw.write(new AlarmDto(result).toString());
            
            //HbaseUtils.showResult(result);
        }
        System.out.println("Total:" + cnt);
        bw.close();
    }
    

    private String decodeUTF8(String s){
    	String result = "decodeFailed";
        try {
        	result = URLDecoder.decode(s.replaceAll("\\\\x", "%"), "utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
        return result;
    }

/*    public int queryDeliveryMessage(String vin, String platCode, String beginTime, String endTime) {

        int msgQty = 0;
        ResultScanner scanner = null;
        try {
            Scan scan = new Scan();
            scan.getFilter();

            String startRowKey = RowKeyUtil.makeDeliveryMessageRowKey("###", vin, beginTime, "02", "0");
            String endRowKey = RowKeyUtil.makeDeliveryMessageRowKey("###", vin, endTime, "03", "1");

            System.out.println(startRowKey);
            System.out.println(endRowKey);
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(endRowKey));

            scan.setCacheBlocks(false);
            scan.setCaching(500);
            scan.setBatch(12);
            scanner = table.getScanner(scan);

            for (Result result : scanner) {
                byte[] platform_code = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("platform_code"));
                byte[] delivery_date = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("collect_date"));

                if ( Bytes.toString(delivery_date).equals("")) {
                    byte[] vinCode = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("vin_code"));
                    byte[] collectDate = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("collect_date"));
                    byte[] message = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("message"));
                    //System.out.println(Bytes.toString(vinCode));
                    //System.out.println(Bytes.toString(collectDate));
                    System.out.println(Bytes.toString(platform_code));
                    System.out.println(Bytes.toString(result.getRow()));
                    msgQty++;
                    System.out.println(msgQty);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            return msgQty;
        }
    }*/


    //根据rowKey过滤数据，rowKey可以使用正则表达式
    //返回rowKey和Cells的键值对
/*    public Map<String, List<Cell>> filterByRowKeyRegex(String tableNameString, String rowKey, CompareOperator operator) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableNameString));
        Scan scan = new Scan();
        //使用正则
        RowFilter filter = new RowFilter(operator, new RegexStringComparator(rowKey));

        //包含子串匹配,不区分大小写。
//        RowFilter filter = new RowFilter(operator,new SubstringComparator(rowKey));

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        Map<String, List<Cell>> map = new HashMap<>();
        for (Result result : scanner) {
            map.put(Bytes.toString(result.getRow()), result.listCells());
        }
        table.close();
        return map;
    }*/

}




