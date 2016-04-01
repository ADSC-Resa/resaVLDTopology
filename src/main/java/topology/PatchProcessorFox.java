package topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import logodetection.Parameters;
import logodetection.SIFTfeatures;
import logodetection.SLM_SIFTLogoState;
import logodetection.StormVideoLogoDetectorGamma;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_nonfree;
import tool.Serializable;
import util.ConfigUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static tool.Constants.*;
import static topology.StormConfigManager.getInt;
import static topology.StormConfigManager.getListOfStrings;

/**
 * Created by Tom Fu at Aug 5, 2015
 * We try to enable to detector more than one logo template file
 * Enable sampling, add sampleID
 * move to Fox version
 */
public class PatchProcessorFox extends BaseRichBolt {
    OutputCollector collector;
    opencv_nonfree.SIFT sift;
    private List<StormVideoLogoDetectorGamma> detectors;
    private HashMap<String, SLM_SIFTLogoState> slmDetectors;
    private int lastExecutedSLMCommandId = 0;
    Parameters parameters;
    int maxAdditionTemp;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        slmDetectors = new HashMap<>();

        int minNumberOfMatches = Math.min(getInt(map, "minNumberOfMatches"), 4);
        this.collector = outputCollector;
        // TODO: get path to logos & parameters from config, different logo can use different threshold?
        parameters = new Parameters()
                .withMatchingParameters(
                        new Parameters.MatchingParameters()
                                .withMinimalNumberOfMatches(minNumberOfMatches)
                );

        sift = new opencv_nonfree.SIFT(0, 3, parameters.getSiftParameters().getContrastThreshold(),
                parameters.getSiftParameters().getEdgeThreshold(), parameters.getSiftParameters().getSigma());

        List<String> templateFiles = new ArrayList<>();
        if(getInt(map, "useOriginalTemplateFiles") != 0)
            templateFiles = getListOfStrings(map, "originalTemplateFileNames");

        maxAdditionTemp = ConfigUtil.getInt(map, "maxAdditionTemp", 4);

        detectors = new ArrayList<>();
        for (int logoIndex = 0; logoIndex < templateFiles.size(); logoIndex ++) {
            detectors.add(new StormVideoLogoDetectorGamma(parameters, templateFiles.get(logoIndex), logoIndex, maxAdditionTemp));
        }
        System.out.println("tPatchProcessorDelta.prepare, with logos: " + detectors.size() + ", maxAdditionTemp: " + maxAdditionTemp);
        opencv_core.IplImage fk = new opencv_core.IplImage();
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if (streamId.equals(PATCH_FRAME_STREAM)) {
            processFrame(tuple);
        }
        else if (streamId.equals(LOGO_TEMPLATE_UPDATE_STREAM)) {
            processNewTemplate(tuple);
        }
        else {
            //Next chunk of code is to make sure we execute the commands in order.
//            int commandID = tuple.getIntegerByField(SLM_FIELD_COMMAND_ID);
//            if (commandID == lastExecutedSLMCommandId + 1) {
//                lastExecutedSLMCommandId++; // OK. Excepted command ID, continue.
//            } else if (commandID > lastExecutedSLMCommandId + 1) {
//                collector.fail(tuple); // Executing a later command than the next one in line
//                return;
//            } else {
//                collector.ack(tuple); // Executing an earlier command (repeated). Send ACK as it's already executed.
//                return;
//            }

            switch (streamId) {
                case SLM_STREAM_ADD_PROCESSED_SIFT_LOGO:
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

        slmDetectors.put(
                logoID,
                new SLM_SIFTLogoState(
                        new StormVideoLogoDetectorGamma(
                                parameters,
                                (Serializable.Mat)tuple.getValueByField(SLM_FIELD_PROCESSED_SIFT_LOGO),
                                maxAdditionTemp
                        ),
                        commandID,
                        SLM_LOGO_STATE_NORMAL
                )
        );

        System.out.println("Added SLM Logo, size: " + slmDetectors.size());
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

        List<Serializable.Rect> detectedLogoList = new ArrayList<>();

        SIFTfeatures sifTfeatures = new SIFTfeatures(sift, identifierMat.sMat.toJavaCVMat(), identifierMat.identifier.roi.toJavaCVRect(), false);
        for (int logoIndex = 0; logoIndex < detectors.size(); logoIndex ++) {
            StormVideoLogoDetectorGamma detector = detectors.get(logoIndex);
            detector.detectLogosByFeatures(sifTfeatures);

            Serializable.Rect detectedLogo = detector.getFoundRect();
            Serializable.Mat extractedTemplate = detector.getExtractedTemplate();
            if (detectedLogo != null) {
                collector.emit(LOGO_TEMPLATE_UPDATE_STREAM,
                        new Values(identifierMat.identifier, extractedTemplate, detector.getParentIdentifier(), logoIndex));
            }

            detectedLogoList.add(detectedLogo);
        }
        collector.emit(DETECTED_LOGO_STREAM, tuple, new Values(frameId, detectedLogoList, patchCount, sampleID));


        //SLM Logos... on a different outwards stream.
        HashMap<String, Serializable.Rect> detectedSLMLogoList = new HashMap<>();
        slmDetectors.forEach((logoID, state)->{
            if(state.getState() == SLM_LOGO_STATE_MUTED) {
                detectedSLMLogoList.put(logoID, null);
            }
            else {
                StormVideoLogoDetectorGamma detector = state.getDetector();
                detector.detectLogosByFeatures(sifTfeatures);

                Serializable.Rect detectedLogo = detector.getFoundRect();
//                Serializable.Mat extractedTemplate = detector.getExtractedTemplate();
//                if (detectedLogo != null) {
//                    collector.emit(LOGO_TEMPLATE_UPDATE_STREAM,
//                            new Values(identifierMat.identifier, extractedTemplate, detector.getParentIdentifier(), logoIndex));
//                }

                detectedSLMLogoList.put(logoID, detectedLogo);
            }
        });

        collector.emit(DETECTED_SIFT_SLM_LOGO_STREAM, tuple, new Values(frameId, detectedSLMLogoList, patchCount, sampleID));
    }

    private void processNewTemplate(Tuple tuple) {
        Serializable.PatchIdentifier receivedPatchIdentifier = (Serializable.PatchIdentifier) tuple.getValueByField(FIELD_HOST_PATCH_IDENTIFIER);
        Serializable.Mat extracted = (Serializable.Mat) tuple.getValueByField(FIELD_EXTRACTED_TEMPLATE);
        Serializable.PatchIdentifier parent = (Serializable.PatchIdentifier) tuple.getValueByField(FIELD_PARENT_PATCH_IDENTIFIER);

        int logoIndex = tuple.getIntegerByField(FIELD_LOGO_INDEX);

        detectors.get(logoIndex).addTemplateBySubMat(receivedPatchIdentifier, extracted);
        detectors.get(logoIndex).incrementPriority(parent, 1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(DETECTED_LOGO_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FOUND_RECT, FIELD_PATCH_COUNT, FIELD_SAMPLE_ID));

        outputFieldsDeclarer.declareStream(DETECTED_SIFT_SLM_LOGO_STREAM, new Fields(FIELD_FRAME_ID, FIELD_FOUND_SLM_RECT, FIELD_PATCH_COUNT, FIELD_SAMPLE_ID));

        outputFieldsDeclarer.declareStream(LOGO_TEMPLATE_UPDATE_STREAM,
                new Fields(FIELD_HOST_PATCH_IDENTIFIER, FIELD_EXTRACTED_TEMPLATE, FIELD_PARENT_PATCH_IDENTIFIER, FIELD_LOGO_INDEX));
    }
}
