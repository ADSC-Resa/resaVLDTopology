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

import java.util.*;

import static tool.Constants.*;

/**
 * Created by Ian.
 */
public class PatchAggregatorHOG extends BaseRichBolt {
    OutputCollector collector;

    /* Keeps track on which patches of the certain frame have already been received */
    Map<Integer, Integer> frameMonitor;

    /* Contains the list of logos found found on a given frame */
    Map< Integer, HashMap<String, List<Serializable.ScoredRect>> > foundRectAccount;
    private int sampleFrames;

    private boolean useNMS;
    private double nmsScore;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        frameMonitor = new HashMap<>();
        foundRectAccount = new HashMap<>();
        useNMS = true;
        sampleFrames = ConfigUtil.getInt(map, "sampleFrames", 1);
        nmsScore = ConfigUtil.getDouble(map, "nmsScoreThreshold", 0.5);
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if(streamId.equals(DETECTED_HOG_SLM_LOGO_STREAM)) {
            processLogoStream(tuple);
        }
        else if(streamId.equals(SLM_STREAM_SET_NMS_HOG_COMMAND)) {
            processSetNMSCommand(tuple);
        }
        else {
            System.out.println("Unexpected stream!");
        }
    }

    private void processSetNMSCommand(Tuple tuple) {
        useNMS = tuple.getBooleanByField(SLM_FIELD_NMS);
        collector.ack(tuple);
    }

    private void processLogoStream(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        int patchCount = tuple.getIntegerByField(FIELD_PATCH_COUNT);
        HashMap<String, List<Serializable.ScoredRect>> foundRect = (HashMap<String, List<Serializable.ScoredRect>>)tuple.getValueByField(FIELD_FOUND_SLM_RECT_LIST);

        if (!foundRectAccount.containsKey(frameId)){
            foundRectAccount.put(frameId, new HashMap<>());

            foundRect.forEach((logoId, rect) -> {
                foundRectAccount.get(frameId).put(logoId, new ArrayList<Serializable.ScoredRect>());
            });
        }

        /* Updating the list of detected logos on the frame */
        foundRect.forEach((id, rectList) -> {
            if(rectList != null) {
                List<Serializable.ScoredRect> account = foundRectAccount.get(frameId).get(id);
                rectList.forEach((rect)->{
                    account.add(rect);
                });
            }
        });

        frameMonitor.computeIfAbsent(frameId, k->0);
        frameMonitor.computeIfPresent(frameId, (k,v)->v+1);;

        sendOutputIfFullyReceived(frameId, patchCount);

        collector.ack(tuple);
    }

    public void sendOutputIfFullyReceived(int frameId, int patchCount) {
        /* If all patches of this frame are collected proceed to the frame aggregator */
        if (frameMonitor.get(frameId) == patchCount) {

            HashMap<String, List<Serializable.ScoredRect>> rectList = foundRectAccount.get(frameId);

            if(useNMS) {
                System.out.println("HOG using NMS");
                // Non-maximum supression
                for (String key : rectList.keySet()) {
                    List<Serializable.ScoredRect> thisList = rectList.get(key);
                    rectList.put(key, doNonMaximumSuppression(thisList, nmsScore));
                }
            }
            else
                System.out.println("HOG NOT using NMS");

            if (frameId % sampleFrames == 0) {
                for (int f = frameId; f < frameId + sampleFrames; f ++) {
                    collector.emit(HOG_PROCESSED_FRAME_STREAM, new Values(f, foundRectAccount.get(frameId)));
                }
            } else { //shall not be here!
                throw new IllegalArgumentException("frameId % sampleFrames != 0, frameID: " + frameId + ", sampleFrames: " + sampleFrames);
            }

            frameMonitor.remove(frameId);
            foundRectAccount.remove(frameId);
        }
    }

    private List<Serializable.ScoredRect> doNonMaximumSuppression(List<Serializable.ScoredRect> inputList, double overlapThreshold) {

        // Convert from Rect to ScoredRect
        ArrayList<Serializable.ScoredRect> boxes = new ArrayList<Serializable.ScoredRect>();
        for(Serializable.ScoredRect i : inputList) {
            boxes.add(i);
        }

        // Do the non-maximum supression

        // Nothing to process
        if(boxes.isEmpty()) {
            return new ArrayList<>();
        }

        ArrayList<Serializable.ScoredRect> results = new ArrayList<Serializable.ScoredRect>();

        ArrayList<Serializable.ScoredRect> sorted = new ArrayList<Serializable.ScoredRect>();
        sorted.addAll(boxes);
        sorted.sort((arg0, arg1) -> Double.compare(arg0.score, arg1.score));

        while(!sorted.isEmpty()) {
            Serializable.ScoredRect last = sorted.get(sorted.size() - 1);
            results.add(last);

            ArrayList<Integer> toDelete = new ArrayList<Integer>();

            for(int i = 0; i < sorted.size(); i++) {
                Serializable.ScoredRect curr = sorted.get(i);

                int mx1 = Math.max(last.getX1(), curr.getX1());
                int my1 = Math.max(last.getY1(), curr.getY1());
                int mx2 = Math.min(last.getX2(), curr.getX2());
                int my2 = Math.min(last.getY2(), curr.getY2());

                int width = Math.max(0, mx2 - mx1 + 1);
                int height = Math.max(0, my2 - my1 + 1);

                double overlap = (double)(width * height) / curr.getArea();

                if(overlap > overlapThreshold) {
                    toDelete.add(i);
                }
            }

            Collections.sort(toDelete);
            Collections.reverse(toDelete);

            for(int i = 0; i < toDelete.size(); i++) {
                sorted.remove((int)toDelete.get(i));
            }
        }

        // Not sure if you can just cast, but let's just do this first
//        ArrayList<Serializable.ScoredRect> resultsToReturn = new ArrayList<>();
//        for(int i = 0; i < results.size(); i++) {
//            resultsToReturn.add(results.get(i));
//        }

        return results;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(HOG_PROCESSED_FRAME_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FOUND_RECT_LIST));
    }
}
