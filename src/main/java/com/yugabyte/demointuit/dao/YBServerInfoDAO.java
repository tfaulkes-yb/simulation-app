package com.yugabyte.demointuit.dao;

import com.yugabyte.demointuit.model.YBServerModel;

import java.util.List;

public interface YBServerInfoDAO {
    List<YBServerModel> getAll();
}
