package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;

import java.util.Map;

import tool.Serializable;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static tool.Constants.*;

/**
 * Created by Ian.
 */
public class LogoProcessorSIFT extends BaseRichBolt {
    OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        System.out.println("Fake image 1");
        opencv_core.IplImage fkimage2 = cvLoadImage("C:\\Users\\Ian\\Desktop\\FYP2\\hog\\1.jpg");
//        opencv_core.IplImage fake = new opencv_core.IplImage();
        System.out.println("Fake image 2");

        System.out.println("Getting sMat SIFT");
        Serializable.Mat mat = (Serializable.Mat)tuple.getValueByField(SLM_FIELD_RAW_LOGO_MAT);

        System.out.println("Fake image 3");

        collector.emit(
                SLM_STREAM_ADD_PROCESSED_SIFT_LOGO,
                tuple,
                new Values(tuple.getIntegerByField(SLM_FIELD_COMMAND_ID), tuple.getStringByField(SLM_FIELD_LOGO_ID) , mat)
        );

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(SLM_STREAM_ADD_PROCESSED_SIFT_LOGO, new Fields(SLM_FIELD_COMMAND_ID, SLM_FIELD_LOGO_ID, SLM_FIELD_PROCESSED_SIFT_LOGO));
    }
}
