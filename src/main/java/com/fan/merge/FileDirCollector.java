package com.fan.merge;

import com.fan.merge.dto.FileCombineDto;
import com.fan.merge.dto.FileCombineRuleDto;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.Jedis;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * com.fan.combine.FIleDirCollector
 * 将用户传递的FileCombineRuleDto转换成 FileCombineDto 并放入redis队列中
 */
public class FileDirCollector {

    public static DateFormat fmt = new SimpleDateFormat("yyyyMMdd");
    public static void main(String[] args) throws Exception{
        AbstractApplicationContext context = new AnnotationConfigApplicationContext(ConfigParser.class);
        JedisPoolz redispools = (JedisPoolz) context.getBean("redispools");
        AppConfig appconfig = (AppConfig) context.getBean("appconfig");
        String fileDirKey = appconfig.getFileDirKey();
        String queneName = appconfig.getQueneName();
        Jedis jedis = redispools.getResource();

        jedis.flushAll();
        addDirs(jedis,fileDirKey);

        //从redis中获得所有需要合并拆分的目录
        List<String> dirs = jedis.lrange(fileDirKey, 0, -1);

        Gson gson = new Gson();
        Configuration conf = new Configuration();

        FileSystem fileSystem = FileSystem.get(conf);

        for(String dir:dirs){
            FileCombineRuleDto fileCombineRuleDto = gson.fromJson(dir,FileCombineRuleDto.class);
            add2redis(fileSystem,fileCombineRuleDto,new Path(fileCombineRuleDto.getFileDir()),jedis,queneName);
        }

    }

    public static void add2redis(FileSystem fileSystem,FileCombineRuleDto fileCombineRuleDto,Path path,
                                 Jedis jedis,String queneName)throws Exception{
        if(isLastDir(fileSystem,path,fileCombineRuleDto)){
            FileCombineDto dto = new FileCombineDto();
            dto.setFileDir(path.toString());
            dto.setType(fileCombineRuleDto.getType());
            dto.setFileSize(fileCombineRuleDto.getFileSize());
            Gson gson = new Gson();
            String json = gson.toJson(dto);
            System.out.println(json);
            jedis.rpush(queneName,json);
        }else{
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            for(FileStatus fileStatusz:fileStatuses){
                if(fileStatusz.isDirectory()){
                    add2redis(fileSystem,fileCombineRuleDto,fileStatusz.getPath(),jedis,queneName);
                }
            }
        }

    }

    /**
     * 判断当前目录下是否全是文件,目前只支持目录下全是文件的情况，文件和目录同在一个目录下的情况暂不考虑
     * @return
     */
    public static boolean isLastDir(FileSystem fileSystem,Path path,FileCombineRuleDto fileCombineRuleDto)throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        Date startDate = fmt.parse(fileCombineRuleDto.getStartTime() + "");
        Calendar ca = Calendar.getInstance();
        ca.add(Calendar.DATE, -1*fileCombineRuleDto.getEndTime());

        long  start = startDate.getTime();
        long end = ca.getTime().getTime();
        int filecount = 0;
        System.out.println(start+"     "+end+"    "+fileCombineRuleDto.getStartTime());
        for(FileStatus fileStatus:fileStatuses){
            if(fileStatus.isDirectory()
//                    ||!(fileStatus.getModificationTime()>start&&fileStatus.getModificationTime()<end)
            ){
                return false;
            }
            filecount ++;
        }
        return filecount>0;
    }

    public static List<FileCombineDto> parseDir(List<String> dirs){
        Gson gson = new Gson();
        List<FileCombineDto> list = new ArrayList<FileCombineDto>();
        for(String dir:dirs){
            FileCombineRuleDto fileCombineRuleDto = gson.fromJson(dir, FileCombineRuleDto.class);
            FileCombineDto fileCombineDto = new FileCombineDto();
            fileCombineDto.setFileDir(fileCombineRuleDto.getFileDir());
            fileCombineDto.setFileSize(fileCombineRuleDto.getFileSize());
            fileCombineDto.setType(fileCombineRuleDto.getType());
            list.add(fileCombineDto);
        }

        return list;
    }

    public static void addDirs(Jedis jedis,String keys){
        FileCombineRuleDto dto = new FileCombineRuleDto();
        dto.setEndTime(5);
        dto.setStartTime(20181001);
        dto.setFileDir("/user/hive/warehouse/user_orc");
        dto.setType("orc");
        dto.setFileSize(5);
        Gson gson = new Gson();
        String json1 = gson.toJson(dto);
        jedis.rpush(keys,json1);
        dto.setType("parquet");
        dto.setFileDir("/user/hive/warehouse/user_parquet");
        jedis.rpush(keys,gson.toJson(dto));
    }

}
