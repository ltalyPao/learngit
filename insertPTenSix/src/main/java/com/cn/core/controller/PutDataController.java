package com.cn.core.controller;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class PutDataController {
    private static List<Long> timeArray =new ArrayList<Long>();
    private static Long[] timelong = {1597769970000L,
            1597773570000L,1597777170000L,
            1597780770000L,1597784370000L,
            1597787970000L,1597791570000L,
            1597795170000L,1597798770000L,
            1597802370000L,1597805970000L,
            1597809570000L,1597813170000L,
            1597816770000L,1597820370000L,
            1597823970000L,1597827570000L,
            1597831170000L,1597834770000L,
            1597838370000L,1597841970000L,
            1597845570000L,1597849170000L,
            1597852770000L};
    private static boolean flag = false;
    static{

        init();

    }
    private static void init(){
        System.out.println("init..now..");

        //addP16();
        //scanP16();
        scanP();
    }
    public static void addP16(){
        System.out.println("addP16..now..");
        Table table = null;
        try {
            table = HbaseConnUtils.getTable("monitordata");
            List<Put> putList = new ArrayList<Put>();
            Put put = null;
            for(int i=0;i<timelong.length;i++){
                put = new Put(Bytes.add(Bytes.toBytes((short) 47),
                        Bytes.toBytes((short) 2),Bytes.toBytes(timelong[i])));
                put.addColumn(Bytes.toBytes("P"),
                        new byte[]{ Byte.valueOf("16")},Bytes.toBytes((float) Math.random()*100+1));
                putList.add(put);
            }
            table.put(putList);
            System.out.println("插入完成");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void scanP16(){
        System.out.println("scanP16..now..");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Table table = null;
        try {
            table = HbaseConnUtils.getTable("monitordata");
            Scan scan = new Scan();/*
            scan.setStartRow(Bytes.add(Bytes.toBytes((short) 47),
                    Bytes.toBytes((short) 2),Bytes.toBytes(timelong[0])));
            System.out.println("startTime:"+sdf.format(new Date(timelong[0])));

            scan.setStopRow(Bytes.add(Bytes.toBytes((short) 47),
            Bytes.toBytes((short) 2),Bytes.toBytes(timelong[timelong.length-1])));
            System.out.println("endTime:"+sdf.format(new Date(timelong[timelong.length-1])));*/
            scan.setStartRow(Bytes.add(Bytes.toBytes((short) 47),
                    Bytes.toBytes((short) 1),Bytes.toBytes(1598457600000L)));

            scan.setStopRow(Bytes.add(Bytes.toBytes((short) 47),
                    Bytes.toBytes((short) 1),Bytes.toBytes(1598543940000L)));

            //scan.setCaching(1000);
            //scan.setReversed(true);
            ResultScanner resultScanner = table.getScanner(scan);
            System.out.println("resultScanner。。。。。");
            for(Result r : resultScanner){

                System.out.println("in。。cicly。。。");

                byte[] row = r.getRow();
                System.out.println("companydId=" + Bytes.toShort(row, 0, 2));
                System.out.println("DeviceId=" + Bytes.toShort(row, 2, 2));
                System.out.println("time=" + sdf.format(new Date(Bytes.toLong(row, 4, 8))));
                if(!flag){
                    timeArray.add(Bytes.toLong(row, 4, 8));
                }

                System.out.println("S0:"+Bytes.toFloat(
                        r.getValue(Bytes.toBytes("P"), new byte[]{Byte.valueOf("16")})));
            }
            /*for (Result r : resultScanner) {
                System.out.println("获得到rowkey:" + r.getRow());
                byte[] row = r.getRow();
                System.out.println("companydId=" + Bytes.toShort(row, 0, 2));
                System.out.println("DeviceId=" + Bytes.toShort(row, 2, 2));
                System.out.println("time=" + sdf.format(new Date(Bytes.toLong(row, 4, 8))));

                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + Bytes.toFloat(keyValue.getValue()));
                }
            }*/
            //flag = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void scanP(){
        System.out.println("scanP..now..");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Table table = null;
        try {
            table = HbaseConnUtils.getTable("monitordata");
            Scan scan = new Scan();
            scan.setTimeRange((long)9, Long.MAX_VALUE);
           /* scan.setStartRow(Bytes.add(Bytes.toBytes(Short.MAX_VALUE),
                    Bytes.toBytes(Short.MAX_VALUE),Bytes.toBytes(timelong[0])));

            scan.setStopRow(Bytes.add(Bytes.toBytes((short)0),
                    Bytes.toBytes((short) 0),Bytes.toBytes(0L)));*/
            scan.setStartRow(Bytes.add(Bytes.toBytes((short) 47),
                    Bytes.toBytes((short) 1),Bytes.toBytes(1599062400000L)));

            scan.setStopRow(Bytes.add(Bytes.toBytes((short) 47),
                    Bytes.toBytes((short) 1),Bytes.toBytes(1599148740000L)));

            scan.addColumn(Bytes.toBytes("P"), new byte[] { Byte.valueOf("16") });

            //scan.setCaching(1000);
            //scan.setReversed(true);
            ResultScanner resultScanner = table.getScanner(scan);
            System.out.println("resultScanner。。。。。");
            /*for(int i=0;i<=1500;i++){

                System.out.println("in。。cicly。。。");
                Result  r= resultScanner.next();
                byte[] row = r.getRow();
                System.out.println("companydId=" + Bytes.toShort(row, 0, 2));
                System.out.println("DeviceId=" + Bytes.toShort(row, 2, 2));
                System.out.println("time=" + sdf.format(new Date(Bytes.toLong(row, 4, 8))));
               *//* if(!flag){
                    timeArray.add(Bytes.toLong(row, 4, 8));
                }*//*
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + Bytes.toFloat(keyValue.getValue()));
                }

            }*/
            for (final Result result : resultScanner ) {
                System.out.println("进入循环");
                for (final Cell cell : result.rawCells()) {
                    final byte[] time_bytes = new byte[8];
                    System.arraycopy(cell.getRowArray(), cell.getRowOffset() + 4, time_bytes, 0, 8);
                    final long time = Bytes.toLong(time_bytes);
                    System.out.println("time=" + sdf.format(new Date(time)));
                    final String[] aliasAndType = "S0,f".split(",");

                    Object value = null;
                    if (cell.getTimestamp() >> 12 != 1L) {
                        if ("d".equals(aliasAndType[1])) {
                            value = Bytes.toDouble(CellUtil.cloneValue(cell));
                        }
                        else if ("f".equals(aliasAndType[1])) {
                            value = Bytes.toFloat(CellUtil.cloneValue(cell));
                            if (aliasAndType[0].toLowerCase().contains("demand")) {
                                value = Bytes.toFloat(CellUtil.cloneValue(cell)) / 1000.0f;
                            }
                        }
                    }
                    System.out.println("值。。。。："+String.valueOf(value));
                }
               /* System.out.println("获得到rowkey:" + result.getRow());
                byte[] row = result.getRow();
                System.out.println("companydId=" + Bytes.toShort(row, 0, 2));
                System.out.println("DeviceId=" + Bytes.toShort(row, 2, 2));
                System.out.println("time=" + sdf.format(new Date(Bytes.toLong(row, 4, 8))));

                for (KeyValue keyValue : result.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====S0值:" + Bytes.toFloat(keyValue.getValue()));
                }*/
            }


            //flag = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
