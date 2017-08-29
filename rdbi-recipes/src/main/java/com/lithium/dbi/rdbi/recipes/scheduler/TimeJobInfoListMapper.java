package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.ResultMapper;

import java.util.List;

public class TimeJobInfoListMapper implements ResultMapper<List<TimeJobInfo>, List<String>>  {

    @Override
    public List<TimeJobInfo> map(List<String> result) {
        return TimeJobInfo.from(new JobInfoListMapper().map(result));
    }
}
