package com.fan.merge.test;

import com.fan.merge.AppConfig;
import com.fan.merge.ConfigParser;
import com.fan.merge.dto.FileCombineDto;
import com.fan.merge.transform.CombineFile;
import com.fan.merge.transform.FileCombineFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.orc.*;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class HDFSTest {
    private final static Logger logger = Logger.getLogger(HDFSTest.class);
    public static void main(String[] args) throws Exception {

        System.out.println(1000*1.0/123>0.5);



    }



    public static long rowCount(Path path,Configuration conf) throws IOException {

        Reader reader = OrcFile.createReader(path,
                OrcFile.readerOptions(conf));
        long numberOfRows = reader.getNumberOfRows();
        return numberOfRows;
    }

    public static void getOrcFileSizeAndRowCount()throws IOException{
        Path inputPath = new Path("/user/hive/warehouse/user_orc/000000_0");
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(inputPath,
                OrcFile.readerOptions(conf));
        long numberOfRows = reader.getNumberOfRows();
        long contentLength = reader.getContentLength();
        logger.info("numberOfRows:"+numberOfRows+"  contentLength:"+contentLength);
    }



    public static void getParquetFileSizeAndRowCount()throws Exception{
        Path inputPath = new Path("/user/hive/warehouse/user_parquet");
        Configuration conf = new Configuration();
        FileStatus[] inputFileStatuses = inputPath.getFileSystem(conf).globStatus(inputPath);

        for (FileStatus fs : inputFileStatuses) {
            for (Footer f : ParquetFileReader.readFooters(conf, fs, false)) {
                for (BlockMetaData b : f.getParquetMetadata().getBlocks()) {
                    logger.info("TotalByteSize:"+b.getTotalByteSize() +"   CompressedSize:"+b.getCompressedSize()+"   rowCount:"+b.getRowCount());
                }
            }
        }
    }

    public void combineParquet()throws Exception{
        AbstractApplicationContext context = new AnnotationConfigApplicationContext(ConfigParser.class);
        AppConfig appconfig = (AppConfig) context.getBean("appconfig");

        FileCombineDto dto = new FileCombineDto();
        dto.setFileSize(100);
        dto.setType("parquet");
        dto.setFileDir("/user/hive/warehouse/user_parquet");



        CombineFile combine = FileCombineFactory.getCombine(dto,appconfig);
//        combine.combine(dto);
    }

    public static void test()throws Exception{
        Configuration conf = new Configuration();
        Path path = new Path("/user/hive/warehouse/user_table/user");
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        IOUtils.copyBytes(inputStream, System.out, 4096, false);
        inputStream.close();
        System.out.println(inputStream.available());
    }





    public static void combineOrc ()throws Exception{
        Configuration conf = new Configuration();
        String src = "/user/hive/warehouse/user_orc";
        List<Path> fileList = getPaths(conf, src);

        String dist = src+"/combine"+System.currentTimeMillis()+".orc";
        Writer writer = null;
        for(Path p:fileList){
            Reader reader = OrcFile.createReader(p,
                    OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            TypeDescription schema = reader.getSchema();
            VectorizedRowBatch batch = schema.createRowBatch();
            if(writer==null){
                writer = OrcFile.createWriter(new Path(dist),
                        OrcFile.writerOptions(conf)
                                .setSchema(schema));
            }

            System.out.println("combine file:"+p);
            while (rows.nextBatch(batch)) {
                if (batch.size != 0) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            rows.close();
        }
        writer.close();
    }


    public static List<Path> getPaths(Configuration conf,String src)throws IOException{
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path(src), true);
        List<Path> fileList = new ArrayList<Path>();
        while(locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path path = next.getPath();
            fileList.add(path);
        }

        return fileList;
    }


    public static MessageType getParquetSchema(FileCombineDto dto )throws IOException{
        Configuration conf = new Configuration();
        ParquetMetadata metaData;

        Path path = new Path(dto.getFileDir());
        FileSystem fs = path.getFileSystem(conf);
        Path file;
        if (fs.isDirectory(path)) {
            FileStatus[] statuses = fs.listStatus(path, HiddenFileFilter.INSTANCE);
            if (statuses.length == 0) {
                throw new RuntimeException("Directory " + path.toString() + " is empty");
            }
            file = statuses[0].getPath();
        } else {
            file = path;
        }
        metaData = ParquetFileReader.readFooter(conf, file, NO_FILTER);
        MessageType schema = metaData.getFileMetaData().getSchema();
        return schema;
    }

}
