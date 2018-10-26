package com.fan.merge.transform;

import com.fan.merge.AppConfig;
import com.fan.merge.contants.Contants;
import com.fan.merge.dto.FileCombineDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class ParquetCombine extends CombineFile {


    @Override
    public long rowCount(Path path, Configuration conf) throws IOException {

        FileStatus[] inputFileStatuses = path.getFileSystem(conf).globStatus(path);
        List<Footer> footers = ParquetFileReader.readFooters(conf, inputFileStatuses[0], false);
        List<BlockMetaData> blocks = footers.get(0).getParquetMetadata().getBlocks();
        long rowcount  = blocks.get(0).getRowCount();
        return rowcount;
    }

    public ParquetCombine(AppConfig appconfig) {
        super(appconfig);
    }

    @Override
    public void combine(FileCombineDto dto, Configuration conf) throws IOException {
        List<Path> paths = getPaths(conf, dto.getFileDir());
        FileSystem fs = FileSystem.get(conf);
        MessageType schema = getParquetSchema(conf,dto);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dto.getFileDir()));
        long totalRowCount = 0;
        long totalSize = 0;
        for(Path path :paths){
            totalRowCount += rowCount(path, conf);
        }

        for(FileStatus fileStatus:fileStatuses){
            totalSize += fileStatus.getLen();
        }

        long fileSize = dto.getFileSize()*1024*1024;

        long  fileCount = totalSize/fileSize>0?totalSize/fileSize:1;
        //每个文件多少行记录
        long fileRows = totalRowCount/fileCount;

        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema,conf);
        String dist = dto.getFileDir()+ File.separator
                + Contants.FILE_PREFIX+System.currentTimeMillis()+"_"+ UUID.randomUUID() +"."+dto.getType();
        ParquetWriter<Group> writer = new ParquetWriter<Group>(new Path(dist),conf,writeSupport);

        int count = 0;
        for(Path path :paths){
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, path);
            ParquetReader<Group> build = reader.build();
            Group line = null;
            while ((line = build.read()) != null) {
                count++;
                if(count%fileRows==0&&(totalRowCount-count)>fileRows*Contants.COMBINE_COEFFICIENT){
                    writer.close();
                    writer = null;
                    logger.info("writed rows:"+count);

                }
                if(writer==null){
                    dist = dto.getFileDir()+ File.separator
                            + Contants.FILE_PREFIX+System.currentTimeMillis()+"_"+ UUID.randomUUID() +"."+dto.getType();
                    writer = new ParquetWriter<Group>(new Path(dist),conf,writeSupport);
                }
                writer.write(line);
            }
        }
        delete(paths,conf);
        writer.close();
    }


    //    @Override
//    public void combine(FileCombineDto dto) throws IOException {
//        Configuration configuration = new Configuration();
//        MessageType schema = this.getParquetSchema(configuration,dto);
//        List<FileCombinePersDto> fileCombinePersDtos = get(configuration, dto);
//
//        GroupWriteSupport writeSupport = new GroupWriteSupport();
//        writeSupport.setSchema(schema,configuration);
//
//        for(FileCombinePersDto fileCombineDto:fileCombinePersDtos){
//            String dist = fileCombineDto.getFileName();
//            Path path = new Path(dist);
//            if(fileCombineDto.getList()!=null){
//                ParquetWriter<Group> writer = new ParquetWriter<Group>(path,configuration,writeSupport);
//                for(FileStatusDto p:fileCombineDto.getList()){
//                    long startTime = System.currentTimeMillis();
//                    logger.info("combining file:"+p);
//
//                    GroupReadSupport readSupport = new GroupReadSupport();
//                    ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, p.getPath());
//
//
//                    ParquetReader<Group> build = reader.build();
//                    Group line = null;
//                    int count = 0;
//                    while ((line = build.read()) != null) {
//                        count++;
//                        if(count%100000==0){
//                            logger.info("count :"+count+"  costs:"+((System.currentTimeMillis()-startTime)/1000)+"s");
//                        }
//                        writer.write(line);
//                    }
//                    logger.info("combining file :"+p+"  costs:"+((System.currentTimeMillis()-startTime)/1000)+"s");
//                }
//                writer.close();
//            }
//
//
//        }
//        logger.info(dto.getFileDir()+" combine successed!");
//        delete(fileCombinePersDtos,configuration);
//    }

}
