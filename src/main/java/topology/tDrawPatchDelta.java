package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Util;
import org.bytedeco.javacpp.opencv_core;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import tool.Serializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;

/**
 * Created by Tom Fu at Mar 24, 2015
 * This bolt is designed to draw found rects on each frame,
 * TODO: leave implementation for multiple target logos (currently one support one logo)
 */
public class tDrawPatchDelta extends BaseRichBolt {
    OutputCollector collector;

    //int lim = 31685; // SONY
    private HashMap<Integer, Serializable.Mat> frameMap;
    private HashMap<Integer, List<List<Serializable.Rect>>> foundSiftRectListStatic;
    private HashMap<Integer, HashMap<String, List<Serializable.Rect>>> foundSiftRectListSLM;
    private HashMap<Integer, HashMap<String, List<Serializable.ScoredRect>>> foundHOGRectList;
    private HashMap<Integer, Boolean> useSiftMap;
    private HashMap<Integer, Boolean> useHogMap;

    private boolean toDraw;

    List<opencv_core.CvScalar> colorList;
    List<opencv_core.CvScalar> colorList2;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

        frameMap = new HashMap<>();
        foundSiftRectListStatic = new HashMap<>();
        foundSiftRectListSLM = new HashMap<>();
        foundHOGRectList = new HashMap<>();
        useSiftMap = new HashMap<>();
        useHogMap = new HashMap<>();

        toDraw = true;

        colorList = new ArrayList<>();
        colorList.add(opencv_core.CvScalar.CYAN);
        colorList.add(opencv_core.CvScalar.MAGENTA);
        colorList.add(opencv_core.CvScalar.YELLOW);
//        colorList.add(opencv_core.CvScalar.BLACK);

        colorList2 = new ArrayList<>();
        colorList2.add(opencv_core.CvScalar.BLUE);
        colorList2.add(opencv_core.CvScalar.RED);
        colorList2.add(opencv_core.CvScalar.GREEN);
//        colorList2.add(opencv_core.CvScalar.GRAY);

    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();

        if(streamId.equals(SLM_STREAM_SET_DRAW_RECTANGLES_COMMAND)) {
            toDraw = tuple.getBooleanByField(SLM_FIELD_DRAW);
            collector.ack(tuple);
            return;
        }

        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        opencv_core.IplImage imageFK = new opencv_core.IplImage();

        if(streamId.equals(PROCESSED_FRAME_STREAM)) {
            List<List<Serializable.Rect>> list1 = (List<List<Serializable.Rect>>) tuple.getValueByField(FIELD_FOUND_RECT_LIST);
            HashMap<String, List<Serializable.Rect>> list2 = (HashMap<String, List<Serializable.Rect>>) tuple.getValueByField(FIELD_FOUND_SLM_RECT_LIST);

            foundSiftRectListStatic.put(frameId, list1);
            foundSiftRectListSLM.put(frameId, list2);
            //System.out.println("PROCESSED_FRAME_STREAM: " + System.currentTimeMillis() + ":" + frameId);
        }
        else if(streamId.equals(HOG_PROCESSED_FRAME_STREAM)) {
            HashMap<String, List<Serializable.ScoredRect>> list = (HashMap<String, List<Serializable.ScoredRect>>) tuple.getValueByField(FIELD_FOUND_RECT_LIST);
            foundHOGRectList.put(frameId, list);
        }
        else if(streamId.equals(FRAME_ALGORITHM_STATUS_STREAM)) {
            useSiftMap.put(frameId, tuple.getBooleanByField(FIELD_USE_SIFT));
            useHogMap.put(frameId, tuple.getBooleanByField(FIELD_USE_HOG));
        }
        else if(streamId.equals(RAW_FRAME_STREAM)) {
            Serializable.Mat sMat = (Serializable.Mat) tuple.getValueByField(FIELD_FRAME_MAT);
            frameMap.put(frameId, sMat);
            //System.out.println("RAW_FRAME_STREAM: " + System.currentTimeMillis() + ":" + frameId);
        }

        if (frameMap.containsKey(frameId)
                && (useSiftMap.containsKey(frameId) && !useSiftMap.get(frameId) || foundSiftRectListStatic.containsKey(frameId)) // implication! x => y == !x or y; also no need to check foundSiftRectListSLM both static and SLM lists always come together
                && (useHogMap.containsKey(frameId) && !useHogMap.get(frameId) || foundHOGRectList.containsKey(frameId)) // implication!
                ) {

            opencv_core.Mat mat = frameMap.get(frameId).toJavaCVMat();

            List<List<Serializable.Rect>> list1;
            HashMap<String, List<Serializable.Rect>> list2;
            if(useSiftMap.get(frameId)) {
                list1 = foundSiftRectListStatic.get(frameId);
                list2 = foundSiftRectListSLM.get(frameId);
            }
            else {
                list1 = new ArrayList<>();
                list2 = new HashMap<>();
            }

            HashMap<String, List<Serializable.ScoredRect>> HOGList;
            if(useHogMap.get(frameId))
                HOGList = foundHOGRectList.get(frameId);
            else
                HOGList = new HashMap<>();

            int colourCounter = 0;

            if(toDraw) {
                // Draw for list1.
                for (int logoIndex = 0; logoIndex < list1.size(); logoIndex++) {

                    opencv_core.CvScalar color = colorList.get(colourCounter++ % colorList.size());
                    if (list1.get(logoIndex) != null) {
//                    System.out.println("FrameDisplay-finishedAdd: " + frameId
//                            + "logo: " + logoIndex + ", of size: " + list1.get(logoIndex).size() + ", " + System.currentTimeMillis());

                        for (Serializable.Rect rect : list1.get(logoIndex)) {
//                        if (logoIndex > 0) {
//                            opencv_core.Rect orgRect = rect.toJavaCVRect();
//                            opencv_core.Rect adjRect = new opencv_core.Rect(orgRect.x() + 5, orgRect.y() + 5, orgRect.width() - 10, orgRect.height() - 10);
//                            Util.drawRectOnMat(adjRect, mat, color);
//                        } else {
//                            Util.drawRectOnMat(rect.toJavaCVRect(), mat, color);
//                        }
                            Util.drawRectOnMat(rect.toJavaCVRect(), mat, color, 2);
                        }
                    }
                }

                // Draw for list2.

                for (Map.Entry<String, List<Serializable.Rect>> i : list2.entrySet()) {
                    opencv_core.CvScalar color = colorList.get(colourCounter++ % colorList.size());
                    if (i.getValue() != null) {
                        for (Serializable.Rect rect : i.getValue()) {
                            Util.drawRectOnMat(rect.toJavaCVRect(), mat, color, 2);
                        }
                    }
                }

                // Draw for HOG
                colourCounter = 0;
                for (Map.Entry<String, List<Serializable.ScoredRect>> i : HOGList.entrySet()) {
                    opencv_core.CvScalar color = colorList2.get(colourCounter++ % colorList2.size());
                    if (i.getValue() != null) {
                        for (Serializable.ScoredRect rect : i.getValue()) {
                            Util.drawRectOnMat(rect.toJavaCVRect(), mat, color, 2);
                        }
                    }
                }
            }

            // Prepare the rectangles (omitting the static ones) to send to redis, as a JSON string
            JSONArray redisRects = new JSONArray();

            list2.forEach((logoId, rectList) -> {
                for(Serializable.Rect rect : rectList) {
                    JSONObject obj = new JSONObject();

                    obj.put("top", rect.y);
                    obj.put("left", rect.x);
                    obj.put("width", rect.width);
                    obj.put("height", rect.height);
                    obj.put("name", logoId);

                    redisRects.add(obj);
                }
            });

            HOGList.forEach((logoId, rectList) -> {
                for(Serializable.Rect rect : rectList) {
                    JSONObject obj = new JSONObject();

                    obj.put("top", rect.y);
                    obj.put("left", rect.x);
                    obj.put("width", rect.width);
                    obj.put("height", rect.height);
                    obj.put("name", logoId);

                    redisRects.add(obj);
                }
            });

            String redisRectsString = redisRects.toJSONString();

            System.out.println("RECT: " + redisRectsString);

            // Send it down!
            Serializable.Mat sMatNew = new Serializable.Mat(mat);
            collector.emit(STREAM_FRAME_DISPLAY, tuple, new Values(frameId, sMatNew, redisRectsString));

            System.out.println("FrameDisplay-finishedAdd: " + frameId +"@" + System.currentTimeMillis());
            foundSiftRectListStatic.remove(frameId);
            foundSiftRectListSLM.remove(frameId);
            foundHOGRectList.remove(frameId);
            useSiftMap.remove(frameId);
            useHogMap.remove(frameId);
            frameMap.remove(frameId);
        } else {
            //System.out.println("tDrawPatchBetaFinished: " + System.currentTimeMillis() + ":" + frameId);
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(STREAM_FRAME_DISPLAY, new Fields(FIELD_FRAME_ID, FIELD_FRAME_MAT, FIELD_OUTPUT_RECTANGLES));
    }
}
