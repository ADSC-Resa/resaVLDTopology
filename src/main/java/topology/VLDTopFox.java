package topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import logodetection.HOGDetector;
import tool.CommandManagerSpout;
import tool.FrameSourceFox;
import tool.RedisFrameOutputFox;
import tool.Serializable;
import util.ConfigUtil;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static tool.Constants.*;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu, a new version based on echoOneGenRIRO!!
 * In the delta version, we enables the feature of supporting the multiple logo input,
 * When the setting in the configuration file includes multiple logo image files,
 * it automatically creates corresponding detector instance
 * Note: we in this version's patchProc bolt (tPatchProcessorDelta), uses the StormVideoLogoDetectorGamma
 * This gamma Detector helps to decrease overhead of multiple logo image files.
 *
 * Through testing, when sampleFrame = 4, it supports up to 25 fps.
 * Updated on April 29, the way to handle frame sampling issue is changed, this is pre-processed by the spout not to
 * send out unsampled frames to the patch generation bolt.
 *
 * Enabling sampling features. Sampling problem is solved!
 * through testing
 */
public class VLDTopFox {


    public static void main(String args[]) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, FileNotFoundException {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = readConfig(args[0]);

        String host = getString(conf, "redis.host");
        int port = getInt(conf, "redis.port");
        String queueName = getString(conf, "tVLDQueueName");

        String nodeToStormQueueName = getString(conf, "slm.nodeToStorm", "nodeToStorm");

        TopologyBuilder builder = new TopologyBuilder();

        // Settings and Logo Manager Spouts/Bolts
        String settingsAndLogoSpout = "tVLDSettingsAndLogoSpout";
        String siftLogoProcessor = "tVLDSiftLogoProcessor";
        String hogLogoProcessor = "tVLDHogLogoProcessor";

        // Frame Handling Spouts/Bolts, some with new names
        String spoutName = "tVLDSpout"; // OLD
        String frameSpout = "tVLDFrameSpout"; // NEW

        String frameAlgorithmDirector = "tVLDFrameAlgorithmDirector";

        String patchGenBolt = "tVLDPatchGen"; // OLD
        String siftPatchGenerator = "tVLDSiftPatchGenerator"; // NEW
        String hogPatchGenerator = "tVLDHogPatchGenerator";

        String patchProcBolt = "tVLDPatchProc"; // OLD
        String siftPatchProcessor = "tVLDSiftPatchProcessor"; // NEW
        String hogPatchProcessor = "tVLDHogPatchProcessor";

        String patchAggBolt = "tVLDPatchAgg"; // OLD
        String siftPatchAggregator = "tVLDSiftPatchAggregator"; // NEW
        String hogPatchAggregator = "tVLDHogPatchAggregator";

        String patchDrawBolt = "tVLDPatchDraw"; // OLD
        String frameResultsDrawer = "tVLDFrameResultsDrawer"; // NEW

        String redisFrameOut = "tVLDRedisFrameOut"; // OLD
        String frameRedisOutput = "tVLDFrameRedisOutput"; // NEW

        // ===== Settings & Logo Manager (SLM) sub-topology

        builder.setSpout(settingsAndLogoSpout, new CommandManagerSpout(host, port, nodeToStormQueueName), getInt(conf, settingsAndLogoSpout + ".parallelism"))
                .setNumTasks(getInt(conf, settingsAndLogoSpout + ".tasks"));

        builder.setBolt(siftLogoProcessor, new LogoProcessorSIFT(), getInt(conf, siftLogoProcessor + ".parallelism"))
                .shuffleGrouping(settingsAndLogoSpout, SLM_STREAM_ADD_COMMAND)
                .setNumTasks(getInt(conf, siftLogoProcessor + ".tasks"));

        builder.setBolt(hogLogoProcessor, new LogoProcessorHOG(), getInt(conf, hogLogoProcessor + ".parallelism"))
                .shuffleGrouping(settingsAndLogoSpout, SLM_STREAM_ADD_COMMAND)
                .setNumTasks(getInt(conf, hogLogoProcessor + ".tasks"));

        // ===== Main topology

        builder.setSpout(spoutName, new FrameSourceFox(host, port, queueName), getInt(conf, spoutName + ".parallelism"))
                .setNumTasks(getInt(conf, spoutName + ".tasks"));

        // ----- Frame-Algorithm Director (Based on the user's dynamic settings, determines which algorithm's sub-topology
        //                                 will receive data to compute. Also informs the result collector (patchDrawBolt)
        //                                 what to expect.)

        builder.setBolt(frameAlgorithmDirector, new FrameAlgorithmDirector(), getInt(conf, frameAlgorithmDirector + ".parallelism"))
                .shuffleGrouping(spoutName, SPOUT_TO_DIRECTOR_STREAM)
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_SET_ALGORITHM_COMMAND)
                .setNumTasks(getInt(conf, frameAlgorithmDirector + ".tasks"));

        // ----- Patch generators (Creates the patches from the frame)

        builder.setBolt(patchGenBolt, new PatchGenFox(), getInt(conf, patchGenBolt + ".parallelism"))
                .shuffleGrouping(frameAlgorithmDirector, SAMPLE_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchGenBolt + ".tasks"));

        builder.setBolt(hogPatchGenerator, new PatchGeneratorHOG(), getInt(conf, hogPatchGenerator + ".parallelism"))
                .shuffleGrouping(frameAlgorithmDirector, HOG_SAMPLE_FRAME_STREAM)
                .setNumTasks(getInt(conf, hogPatchGenerator + ".tasks"));

        // ----- Patch processors (takes each patch and runs the respective CV algorithm with the current list of logo templates.
        //                         Notice it also takes in some commands from the SLM too, for logo management.)

        builder.setBolt(patchProcBolt, new PatchProcessorFox(), getInt(conf, patchProcBolt + ".parallelism"))
                .allGrouping(patchProcBolt, LOGO_TEMPLATE_UPDATE_STREAM)
                .allGrouping(siftLogoProcessor, SLM_STREAM_ADD_PROCESSED_SIFT_LOGO) // THIS
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_DELETE_COMMAND) // THIS
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_MUTE_COMMAND) // THIS
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_UNMUTE_COMMAND) // THIS
                .shuffleGrouping(patchGenBolt, PATCH_FRAME_STREAM)
                .setNumTasks(getInt(conf, patchProcBolt + ".tasks"));

        builder.setBolt(hogPatchProcessor, new PatchProcessorHOG(), getInt(conf, hogPatchProcessor + ".parallelism"))
                .allGrouping(hogLogoProcessor, SLM_STREAM_ADD_PROCESSED_HOG_LOGO)
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_DELETE_COMMAND)
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_MUTE_COMMAND)
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_UNMUTE_COMMAND)
                .shuffleGrouping(hogPatchGenerator, HOG_PATCH_FRAME_STREAM)
                .setNumTasks(getInt(conf, hogPatchProcessor + ".tasks"));

        // ----- Patch aggregators (collects (gathers) all the logo matches (i.e. rectangles) from all patch processors for
        //                          each frame. Holds onto the logo matches until all patches are accounted for (for both
        //                          algorithms, for each frame), then it runs the respective non-maximum suppression algorithms,
        //                          and finally sends the entire frame's matches down to the patchDrawBolt).

        builder.setBolt(patchAggBolt, new PatchAggFox(), getInt(conf, patchAggBolt + ".parallelism"))
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_SET_NMS_SIFT_COMMAND)
                .fieldsGrouping(patchProcBolt, DETECTED_LOGO_STREAM, new Fields(FIELD_SAMPLE_ID))
                .fieldsGrouping(patchProcBolt, DETECTED_SIFT_SLM_LOGO_STREAM, new Fields(FIELD_SAMPLE_ID))
                .setNumTasks(getInt(conf, patchAggBolt + ".tasks"));

        builder.setBolt(hogPatchAggregator, new PatchAggregatorHOG(), getInt(conf, hogPatchAggregator + ".parallelism"))
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_SET_NMS_HOG_COMMAND)
                .fieldsGrouping(hogPatchProcessor, DETECTED_HOG_SLM_LOGO_STREAM, new Fields(FIELD_SAMPLE_ID))
                .setNumTasks(getInt(conf, hogPatchAggregator + ".tasks"));

        // ----- Results drawer. (Draws results rectangles on each frame, which can be switched on or off; subscribes
        //                        to a SLM stream specifically for this setting.)

        builder.setBolt(patchDrawBolt, new tDrawPatchDelta(), getInt(conf, patchDrawBolt + ".parallelism"))
                .allGrouping(settingsAndLogoSpout, SLM_STREAM_SET_DRAW_RECTANGLES_COMMAND)
                .fieldsGrouping(patchAggBolt, PROCESSED_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(hogPatchAggregator, HOG_PROCESSED_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(spoutName, RAW_FRAME_STREAM, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping(frameAlgorithmDirector, FRAME_ALGORITHM_STATUS_STREAM, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, patchDrawBolt + ".tasks"));

        // ----- Writes the frame and the result-rectangles list into Redis

        builder.setBolt(redisFrameOut, new RedisFrameOutputFox(), getInt(conf, redisFrameOut + ".parallelism"))
                .globalGrouping(patchDrawBolt, STREAM_FRAME_DISPLAY)
                .setNumTasks(getInt(conf, redisFrameOut + ".tasks"));

        // ===== Topology done.

        StormTopology topology = builder.createTopology();

        int numberOfWorkers = getInt(conf, "tVLDNumOfWorkers");
        conf.setNumWorkers(numberOfWorkers);
        conf.setMaxSpoutPending(getInt(conf, "tVLDMaxPending"));

        conf.setStatsSampleRate(1.0);
        conf.registerSerialization(Serializable.Mat.class);
        conf.registerSerialization(HOGDetector.class);
        int sampleFrames = getInt(conf, "sampleFrames");
        int W = ConfigUtil.getInt(conf, "width", 640);
        int H = ConfigUtil.getInt(conf, "height", 480);

        List<String> templateFiles = new ArrayList<>();
        if(getInt(conf, "useOriginalTemplateFiles") != 0)
            templateFiles = getListOfStrings(conf, "originalTemplateFileNames");

        StormSubmitter.submitTopology("VLDTopFox-s" + sampleFrames + "-" + W + "-" + H + "-L" + templateFiles.size(), conf, topology);

    }
}
