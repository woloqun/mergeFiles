package com.fan.merge.transform;

import com.fan.merge.AppConfig;
import com.fan.merge.dto.FileCombineDto;
public class FileCombineFactory {


    public static CombineFile getCombine(FileCombineDto dto, AppConfig appconfig){
        if("orc".equals(dto.getType())){
            return new OrcCombine(appconfig);
        }else if("parquet".equals(dto.getType())){
            return new ParquetCombine(appconfig);
        }
        return null;
    }
}
