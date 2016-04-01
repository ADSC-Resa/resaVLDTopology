package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import tool.Serializable;
import util.ConfigUtil;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Ian.
 */
public class FrameAlgorithmDirector extends BaseRichBolt {
    OutputCollector collector;
    private boolean useSift;
    private boolean useHog;
    private int sampleFrames;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        useSift = true;
        useHog = true;

        sampleFrames = ConfigUtil.getInt(map, "sampleFrames", 1);
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if(streamId.equals(SLM_STREAM_SET_ALGORITHM_COMMAND)) {
            useSift = tuple.getBooleanByField(SLM_FIELD_USE_SIFT);
            useHog = tuple.getBooleanByField(SLM_FIELD_USE_HOG);
        }
        else if(streamId.equals(SPOUT_TO_DIRECTOR_STREAM)) {
            int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
            int sampleId = tuple.getIntegerByField(FIELD_SAMPLE_ID);
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);

            if(useSift)
                collector.emit(SAMPLE_FRAME_STREAM, tuple, new Values(frameId, sMat, sampleId));
            if(useHog)
                collector.emit(HOG_SAMPLE_FRAME_STREAM, tuple, new Values(frameId, sMat, sampleId));

            for (int f = frameId; f < frameId + sampleFrames; f++) {
                collector.emit(FRAME_ALGORITHM_STATUS_STREAM, tuple, new Values(f, useSift, useHog));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(FRAME_ALGORITHM_STATUS_STREAM, new Fields(FIELD_FRAME_ID, FIELD_USE_SIFT, FIELD_USE_HOG));
        outputFieldsDeclarer.declareStream(SAMPLE_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_SAMPLE_ID)); // SIFT
        outputFieldsDeclarer.declareStream(HOG_SAMPLE_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_SAMPLE_ID)); // HOG
    }
}