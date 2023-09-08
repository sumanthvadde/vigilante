package com.github.sumanthvadde;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.sumanthvadde.dto.Alert;

public class AlertSink implements SinkFunction<Alert> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertSink.class);

    public AlertSink() {
    }

    public void invoke(Alert value, SinkFunction.Context context) {
        LOG.info(value.toString());
    }
}