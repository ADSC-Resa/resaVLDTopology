package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.*;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_highgui;
import tool.Serializable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;
import static util.ConfigUtil.*;

/**
 * Created by Ian.
 */
public class PatchProcessorHOG extends BaseRichBolt {
    OutputCollector collector;
    private HashMap<String, SLM_HOGLogoState> slmDetectors;
    private double similarityCutoff;
    private int maximumMatches;
    private int maxWidth, maxHeight;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
//        System.out.println("Fake Image 1A");
//        opencv_core.Mat mat = opencv_highgui.imread(tool.Constants.FAKE_IMAGE, opencv_highgui.CV_LOAD_IMAGE_COLOR);
//
//        System.out.println("Fake Image 1B");
//        opencv_objdetect.HOGDescriptor hog2 = new opencv_objdetect.HOGDescriptor(
//                new opencv_core.Size(64, 64),
//                new opencv_core.Size(16, 16),
//                new opencv_core.Size(8, 8),
//                new opencv_core.Size(8, 8),
//                9
//        );
//
//        System.out.println("Fake Image 1C");
//        hog2.compute(mat, new FloatPointer());
//
//        System.out.println("Fake Image 1D - END");

        System.out.println("Initializing fake image - " + this.getClass().getCanonicalName());
        opencv_core.IplImage fakeImage = opencv_highgui.cvLoadImage(tool.Constants.FAKE_IMAGE);
        System.out.println("Done initializing fake image - " + this.getClass().getCanonicalName());

        this.collector = outputCollector;

        slmDetectors = new HashMap<>();
        similarityCutoff = getDouble(map, "hog.similarityCutoff", 0.65);
        maximumMatches = getInt(map, "hog.maximumMatches", 15);
        maxWidth = (int)(getInt(map, "width", 640) * getDouble(map, "tVLDHogPatchGenerator.hogPatchDivisionX", 0.5));
        maxHeight = (int)(getInt(map, "height", 480) * getDouble(map, "tVLDHogPatchGenerator.hogPatchDivisionY", 0.5));
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(HOG_PATCH_FRAME_STREAM)) {
            processFrame(tuple);
        }
        else {
            switch (streamId) {
                case SLM_STREAM_ADD_PROCESSED_HOG_LOGO:
                    processSLMAddLogo(tuple);
                    break;
                case SLM_STREAM_DELETE_COMMAND:
                    processSLMDeleteLogo(tuple);
                    break;
                case SLM_STREAM_MUTE_COMMAND:
                    processSLMMuteLogo(tuple);
                    break;
                case SLM_STREAM_UNMUTE_COMMAND:
                    processSLMUnmuteLogo(tuple);
                    break;
            }
        }

        collector.ack(tuple);
    }

    private void processSLMAddLogo(Tuple tuple) {
        String logoID = tuple.getStringByField(SLM_FIELD_LOGO_ID);
        int commandID = tuple.getIntegerByField(SLM_FIELD_COMMAND_ID);

        HOGDetector hog = (HOGDetector)tuple.getValueByField(SLM_FIELD_PROCESSED_HOG_LOGO);
        //opencv_core.Mat template = ((Serializable.Mat)tuple.getValueByField(SLM_FIELD_PROCESSED_HOG_LOGO)).toJavaCVMat();
        //HOGDetector hog = new HOGDetector(template, maxWidth, maxHeight, 1.18);

        slmDetectors.put(
                logoID,
                new SLM_HOGLogoState(
                        hog,
                        commandID,
                        SLM_LOGO_STATE_NORMAL
                )
        );

        System.out.println("Added SLM HOG Logo, size: " + slmDetectors.size());
    }

    private void processSLMDeleteLogo(Tuple tuple) {
        String logoID = tuple.getStringByField(SLM_FIELD_LOGO_ID);
        slmDetectors.remove(logoID);
    }

    private void processSLMMuteLogo(Tuple tuple) {
        String logoID = tuple.getStringByField(SLM_FIELD_LOGO_ID);
        slmDetectors.get(logoID).setState(SLM_LOGO_STATE_MUTED);
    }

    private void processSLMUnmuteLogo(Tuple tuple) {
        String logoID = tuple.getStringByField(SLM_FIELD_LOGO_ID);
        slmDetectors.get(logoID).setState(SLM_LOGO_STATE_NORMAL);
    }

    private void processFrame(Tuple tuple) {
        int frameId = tuple.getIntegerByField(FIELD_FRAME_ID);
        int sampleID = tuple.getIntegerByField(FIELD_SAMPLE_ID);

        Serializable.PatchIdentifierMat identifierMat = (Serializable.PatchIdentifierMat) tuple.getValueByField(FIELD_PATCH_FRAME_MAT);
        int patchCount = tuple.getIntegerByField(FIELD_PATCH_COUNT);

        HashMap<String, List<Serializable.ScoredRect>> detectedSLMLogoList = new HashMap<>();
        slmDetectors.forEach((logoID, state)->{
            if(state.getState() == SLM_LOGO_STATE_MUTED) {
                detectedSLMLogoList.put(logoID, null);
            }
            else {
                List<Serializable.ScoredRect> detectedLogos = state.getDetector().match(
                        identifierMat.sMat.toJavaCVMat(),
                        similarityCutoff,
                        maximumMatches
                );

                System.out.println("HOG Patch Processor Detected " + detectedLogos.size() );

                // Add offset
                int xOffset = identifierMat.identifier.roi.x;
                int yOffset = identifierMat.identifier.roi.y;

                // TODO Remove this when HOG library problems are solved
//                // We check if it's the first patch, then just add a small (random) rectangle just to double check the
//                // algorithm for the topology is working correctly.
//
//                int randomLocationX = Math.abs(logoID.hashCode() % 100);
//                int randomLocationY = Math.abs(logoID.hashCode() % 10000 / 100);
//
//                detectedLogos.add(new Serializable.ScoredRect(randomLocationX, randomLocationY, 20, 20, 0.9));

                // TODO End-of-Remove

                for(Serializable.ScoredRect r : detectedLogos) {
//                    System.out.println("original: " + r.x + " " + r.y);
//                    System.out.println("xyOffset: " + xOffset + " " + yOffset);

                    r.x = r.x + xOffset;
                    r.y = r.y + yOffset;

//                    System.out.println("new     : " + r.x + " " + r.y);
                }

                detectedSLMLogoList.put(logoID, detectedLogos);
            }
        });

        collector.emit(DETECTED_HOG_SLM_LOGO_STREAM, tuple, new Values(frameId, detectedSLMLogoList, patchCount, sampleID));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DETECTED_HOG_SLM_LOGO_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FOUND_SLM_RECT_LIST, FIELD_PATCH_COUNT, FIELD_SAMPLE_ID));
    }
}
