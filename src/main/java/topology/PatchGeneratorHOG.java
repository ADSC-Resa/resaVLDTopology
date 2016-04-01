package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.bytedeco.javacpp.opencv_core;
import tool.Serializable;
import static util.ConfigUtil.*;

import java.util.Map;

import static tool.Constants.*;
import static tool.Constants.FIELD_PATCH_COUNT;
import static tool.Constants.FIELD_SAMPLE_ID;

/**
 * Created by Ian.
 */
public class PatchGeneratorHOG extends BaseRichBolt {
    OutputCollector collector;
    double hogPatchDivisionX;
    double hogPatchDivisionY;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        hogPatchDivisionX = getDouble(map, "tVLDHogPatchGenerator.hogPatchDivisionX", 0.5);
        hogPatchDivisionY = getDouble(map, "tVLDHogPatchGenerator.hogPatchDivisionY", 0.5);
    }

    @Override
    public void execute(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        int sampleID = tuple.getIntegerByField(FIELD_SAMPLE_ID);
        Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
        opencv_core.IplImage fk = new opencv_core.IplImage();

        double fx = hogPatchDivisionX, fy = hogPatchDivisionY;
        double fsx = .5, fsy = .5;

        int W = sMat.getCols(), H = sMat.getRows();
        int w = (int) (W * fx + .5), h = (int) (H * fy + .5);
        int dx = (int) (w * fsx + .5), dy = (int) (h * fsy + .5);
        int patchCount = 0;
        for (int x = 0; x + w <= W; x += dx)
            for (int y = 0; y + h <= H; y += dy)
                patchCount++;

        for (int x = 0; x + w <= W; x += dx) {
            for (int y = 0; y + h <= H; y += dy) {
                Serializable.Rect rect = new Serializable.Rect(x, y, w, h);

                opencv_core.Mat pMat = new opencv_core.Mat(sMat.toJavaCVMat(), rect.toJavaCVRect());
                Serializable.Mat pSMat = new Serializable.Mat(pMat);
                Serializable.PatchIdentifierMat subPatchMat = new Serializable.PatchIdentifierMat(frameId, rect, pSMat);

                collector.emit(HOG_PATCH_FRAME_STREAM, tuple, new Values(frameId, subPatchMat, patchCount, sampleID));
            }
        }
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(HOG_PATCH_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_PATCH_FRAME_MAT, FIELD_PATCH_COUNT, FIELD_SAMPLE_ID));
    }
}
