package com.fan.merge.transform;

import com.fan.merge.AppConfig;
import com.fan.merge.contants.Contants;
import com.fan.merge.dto.FileCombineDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class OrcCombine extends CombineFile {

    public OrcCombine(AppConfig appconfig) {
        super(appconfig);
    }

    @Override
    public long rowCount(Path path,Configuration conf) throws IOException {

        Reader reader = OrcFile.createReader(path,
                OrcFile.readerOptions(conf));
        long numberOfRows = reader.getNumberOfRows();
        return numberOfRows;
    }


    @Override
    public void combine(FileCombineDto dto, Configuration conf) throws IOException {
        List<Path> paths = getPaths(conf, dto.getFileDir());
        FileSystem fs = FileSystem.get(conf);
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
        Writer writer = null;

        int curcount = 0;
        int count = 0;
        for(Path path :paths){
            Reader reader = OrcFile.createReader(path,
                    OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows();
            TypeDescription schema = reader.getSchema();
            VectorizedRowBatch batch = schema.createRowBatch();

            while (rows.nextBatch(batch)) {
                count += batch.size;
                curcount += batch.size;

                if(writer==null){
                    String dist = dto.getFileDir()+ File.separator
                            + Contants.FILE_PREFIX+System.currentTimeMillis()+"_"+ UUID.randomUUID() +"."+dto.getType();
                    writer = OrcFile.createWriter(new Path(dist),
                            OrcFile.writerOptions(conf)
                                    .setSchema(schema));
                }

                if (batch.size != 0) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }

                if(curcount>fileRows&&(totalRowCount-count)>fileRows*Contants.COMBINE_COEFFICIENT){
                    writer.close();
                    writer = null;
                    curcount = 0 ;
                }
            }
            rows.close();
        }

        writer.close();
        delete(paths,conf);
    }

    //    @Override
//    public void combine(FileCombineDto dto,Configuration conf) throws IOException {
//        List<FileCombinePersDto> fileCombinePersDtos = get(conf, dto);
//
//        for(FileCombinePersDto fileCombineDto:fileCombinePersDtos){
//            if(fileCombineDto.getList()!=null){
//                String dist = fileCombineDto.getFileName();
//                Writer writer = null;
//                for(FileStatusDto p:fileCombineDto.getList()){
//                    Reader reader = OrcFile.createReader(p.getPath(),
//                            OrcFile.readerOptions(conf));
//                    RecordReader rows = reader.rows();
//                    TypeDescription schema = reader.getSchema();
//                    VectorizedRowBatch batch = schema.createRowBatch();
//                    if(writer==null){
//                        writer = OrcFile.createWriter(new Path(dist),
//                                OrcFile.writerOptions(conf)
//                                        .setSchema(schema));
//                    }
//
//                    logger.info("combining file:"+p);
//                    while (rows.nextBatch(batch)) {
//                        if (batch.size != 0) {
//                            writer.addRowBatch(batch);
//                            batch.reset();
//                        }
//                    }
//                    rows.close();
//                }
//                writer.close();
//            }
//
//        }
//        logger.info(dto.getFileDir()+" combine successed!");
////        delete(fileCombinePersDtos,conf);
//    }

}
