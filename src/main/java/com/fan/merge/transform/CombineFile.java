package com.fan.merge.transform;

import com.fan.merge.AppConfig;
import com.fan.merge.contants.Contants;
import com.fan.merge.dto.FileCombineDto;
import com.fan.merge.dto.FileCombinePersDto;
import com.fan.merge.dto.FileStatusDto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public abstract class CombineFile {

    private AppConfig appconfig;

    public static Logger logger = LogManager.getLogger(CombineFile.class);
    abstract public void combine(FileCombineDto dto, Configuration conf)throws IOException ;
    abstract public long rowCount(Path path,Configuration conf) throws IOException;

    public  List<Path> getPaths(Configuration conf,String src)throws IOException{
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(src));

        List<Path> fileList = new ArrayList<Path>();
        for(FileStatus fileStatus:fileStatuses){
            Path path = fileStatus.getPath();
            fileList.add(path);
        }
        return fileList;
    }


    public long[] getFileSizeAndRows(Configuration conf, FileCombineDto dto)throws IOException{
        FileSystem fs = FileSystem.get(conf);

        long totalSize = 0;
        long rows = 0;

        FileStatus[] inputFileStatuses = fs.globStatus(new Path(dto.getFileDir()));

        for (FileStatus fss : inputFileStatuses) {
            for (Footer f : ParquetFileReader.readFooters(conf, fss, false)) {
                for (BlockMetaData b : f.getParquetMetadata().getBlocks()) {
                    totalSize +=b.getTotalByteSize();
                    rows += b.getRowCount();
//                    logger.info("TotalByteSize:"+b.getTotalByteSize() +"   CompressedSize:"+b.getCompressedSize());
                }
            }
        }
        return new long[]{totalSize,rows};
    }


    public  List<FileCombinePersDto> get(Configuration conf, FileCombineDto dto)throws IOException{
        FileSystem fs = FileSystem.get(conf);

        Path tmpdir = mkdir(conf, dto);
        logger.info("create tmp dir :"+tmpdir);
        //将数据复制一份到tmp.dir
        logger.info(String.format("copy dir from [%s] to [%s]",dto.getFileDir(),tmpdir));
        copyFile(fs,new Path(dto.getFileDir()),tmpdir,conf);

        FileStatus[] fileStatuses = fs.listStatus(new Path(dto.getFileDir()));
        List<FileCombinePersDto> list = new ArrayList<FileCombinePersDto>();
        int fileSize = dto.getFileSize()*1024*1024;

        int totalSize = 0;

        List<FileStatusDto> fileList = new ArrayList<FileStatusDto>();
        for(FileStatus fileStatus:fileStatuses){
            FileStatusDto d = new FileStatusDto(fileStatus.getPath(),fileStatus.getLen(),fileStatus.getModificationTime());
            totalSize += fileStatus.getLen();
            fileList.add(d);
        }
        //sort by fileSize desc
        Collections.sort(fileList);

        int fileCount = totalSize/fileSize>0?totalSize/fileSize:1;
        fileCount = fileCount>fileStatuses.length? fileStatuses.length:fileCount;

        logger.info(String.format("totalSize[%s]  fileSize[%s]   fileCount[%s]", totalSize, fileSize, fileCount));
        for(int i=0;i<fileCount;i++){
            list.add(new FileCombinePersDto(dto.getFileDir()+ File.separator
                    + Contants.FILE_PREFIX+System.currentTimeMillis()+"_"+ UUID.randomUUID() +"."+dto.getType()));
        }

        for(FileStatusDto fileStatus:fileList){
            Collections.sort(list);
            list.get(0).add(fileStatus);
        }
        logger.info(list);

        return list;
    }


    public void copyFile(FileSystem fs ,Path src,Path dist,Configuration conf)throws IOException{
        String dir = src.getName();
        String distDir = dist.toString()+File.separator+dir;
        //如果缓存目录数据存在,删除重建
        if(fs.exists(new Path(distDir))){
            logger.info(String.format("file [%s] is exists,delete file and mkdir later",distDir));
            fs.delete(new Path(distDir),true);
        }
        FileUtil.copy(fs, src, fs,dist, false, conf);
    }




    public Path mkdir(Configuration conf, FileCombineDto dto) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String src = dto.getFileDir();
        src = src.substring(0,src.lastIndexOf(File.separator));
        String dist = appconfig.getTmpDir()+File.separator+src;

        logger.info("dist :"+dist);
        Path path = new Path(dist);

        //如果目录不存在，则创建
        if(!fs.exists(path)){
            logger.info("mkdir :"+path);
            fs.mkdirs(path);
        }

        return path;
    }


    public void delete(List<Path> paths,Configuration conf)throws IOException{
        FileSystem fs = FileSystem.get(conf);
        for(Path path:paths){
            fs.delete(path,true);
            logger.info("delete src file["+path+"]");
        }
    }


    public MessageType getParquetSchema(Configuration conf,FileCombineDto dto)throws IOException{
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
            logger.info(file);
        } else {
            file = path;
        }

        HadoopInputFile inputFile = HadoopInputFile.fromPath(file,conf);
        ParquetFileReader parquetFileReader = ParquetFileReader.open(inputFile);

        metaData = parquetFileReader.getFooter();
        MessageType schema = metaData.getFileMetaData().getSchema();
        logger.info("get parquet file schema:"+schema);
        return schema;

    }

    public CombineFile(AppConfig appconfig) {
        this.appconfig = appconfig;
    }
}
