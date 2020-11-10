package com.ksr;

import com.typesafe.config.Config;

public interface Action {
    void execute(Config conf);
}
