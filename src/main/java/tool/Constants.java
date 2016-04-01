package tool;

/**
 * Created by ding on 14-7-3.
 */

public interface Constants {

    public static String STREAM_FRAME_OUTPUT = "stream-frame";
    public static String ORIGINAL_FRAME_OUTPUT = "org-frame";
    public static String STREAM_FRAME_DISPLAY = "stream-display";
    public static String STREAM_FRAME_FV = "stream-fv";
    public static String STREAM_OPT_FLOW = "stream-optical-flow";
    public static String STREAM_GREY_FLOW = "stream-grey-flow";
    public static String STREAM_EIG_FLOW = "stream-eig-flow";
    public static String STREAM_FEATURE_FLOW = "stream-fea-flow";
    public static String STREAM_RAW_FRAME = "stream-raw-frm";
    public static String STREAM_NEW_TRACE = "stream-new-trace";
    public static String STREAM_EXIST_TRACE = "stream-exist-trace";
    public static String STREAM_REGISTER_TRACE = "stream-register-trace";
    public static String STREAM_RENEW_TRACE = "stream-renew-trace";
    public static String STREAM_INDICATOR_TRACE = "stream-ind-trace";
    public static String STREAM_REMOVE_TRACE = "stream-remove-trace";
    public static String STREAM_EXIST_REMOVE_TRACE = "stream-e-r-trace";
    public static String STREAM_PLOT_TRACE = "stream-plot-trace";
    public static String STREAM_FEATURE_TRACE = "stream-fea-trace";

    public static String STREAM_CACHE_CLEAN = "stream-cache-clean";

    public static String FIELD_FRAME_ID = "frame-id";
    public static String FIELD_SAMPLE_ID = "sample-id";
    public static String FIELD_FRAME_BYTES = "frm-bytes";
    public static String FIELD_FRAME_MAT = "frm-mat";
    public static String FIELD_OPT_MAT = "opt-mat";
    public static String FIELD_FRAME_MAT_PREV = "frm-mat-prev";
    public static String FIELD_FRAME_MAT_ORG = "frm-mat-org";
    public static String FIELD_TRACE_CONTENT = "trace-content";
    public static String FIELD_TRACE_ID = "trace-id";
    public static String FIELD_TRACE_RECORD = "trace-record";
    public static String FIELD_TRACE_META_LAST_POINT = "trace-meta-lp";
    public static String FIELD_COUNTERS_INDEX = "counters-index";
    public static String FIELD_NEW_POINTS = "new-pts";
    public static String FIELD_WIDTH_HEIGHT = "wid-hei";
    public static String FIELD_EIG_INFO = "eig-info";
    public static String FIELD_FLOW_IMPL = "flow-impl";
    public static String FIELD_MBHX_MAT = "mbhx-mat";
    public static String FIELD_MBHY_MAT = "mbhy-mat";
    public static String FIELD_HOG_MAT = "hog-mat";
    public static String FIELD_MBH_HOG_MAT = "mbh-hog-mat";
    public static String FIELD_FEA_VEC = "fea-vec";
    ////////////////For logo detection

    public final static String PATCH_STREAM = "patch-stm";
    public final static String RAW_FRAME_STREAM = "raw-frm-stm";
    public final static String SAMPLE_FRAME_STREAM = "samp-frm-stm";
    public final static String PATCH_FRAME_STREAM = "pat-frm-stm";
    public final static String LOGO_TEMPLATE_UPDATE_STREAM = "ltu-stream";
    public final static String DETECTED_LOGO_STREAM = "dectlogo-stream";
    public final static String CACHE_CLEAR_STREAM = "cc-stream";
    public final static String PROCESSED_FRAME_STREAM = "pf-stream";

    public final static String FIELD_PATCH_FRAME_MAT = "p-frm-mat";
    public final static String FIELD_PATCH_COUNT = "patch-cnt";
    public final static String FIELD_PATCH_IDENTIFIER = "p-ident";
    public final static String FIELD_HOST_PATCH_IDENTIFIER = "host-p-ident";
    public final static String FIELD_DETECTED_LOGO_RECT = "detect-logo-rect";
    public final static String FIELD_FOUND_RECT = "found-rect";
    public final static String FIELD_FOUND_RECT_LIST = "found-rect-list";
    public final static String FIELD_PARENT_PATCH_IDENTIFIER = "par-p-ident";

    public final static String FIELD_EXTRACTED_TEMPLATE = "ext-temp";
    public final static String FIELD_LOGO_INDEX = "logo-index";

    // ===== SLM
    public final static String SLM_FIELD_LOGO_ID = "logo_manager_logo_id";
    public final static String SLM_FIELD_RAW_LOGO_MAT = "field-lm-raw-logo-data";

    public final static String SLM_FIELD_COMMAND_ID = "field-command-id";

    public final static String SLM_STREAM_ADD_COMMAND = "setting-stream-add-command";
    public final static String SLM_STREAM_DELETE_COMMAND = "setting-stream-delete-command";
    public final static String SLM_STREAM_MUTE_COMMAND = "mute_mute_command_stream";
    public final static String SLM_STREAM_UNMUTE_COMMAND = "mute_unmute_command_stream";

    public final static String SLM_STREAM_SET_ALGORITHM_COMMAND = "set-algorithm-command-stream";
    public final static String SLM_FIELD_USE_SIFT = "use-sift-setting";
    public final static String SLM_FIELD_USE_HOG = "use-hog-setting";

    public final static String SLM_STREAM_SET_DRAW_RECTANGLES_COMMAND = "set-draw-rectangles-command-stream";
    public final static String SLM_FIELD_DRAW = "draw-setting";

    public final static String SLM_STREAM_SET_NMS_SIFT_COMMAND = "slm-stream-set-nms-sift-command";
    public final static String SLM_STREAM_SET_NMS_HOG_COMMAND = "slm-stream-set-nms-hog-command";
    public final static String SLM_FIELD_NMS = "slm-field-nms";

    public static final String SLM_STREAM_ADD_PROCESSED_SIFT_LOGO = "slm-stream-add-processed-sift-logo";
    public static final String SLM_FIELD_PROCESSED_SIFT_LOGO = "slm-field-processed-sift-logo";
    
    public static final String SLM_STREAM_ADD_PROCESSED_HOG_LOGO = "slm-stream-add-processed-hog-logo";
    public static final String SLM_FIELD_PROCESSED_HOG_LOGO = "slm-field-processed-hog-logo";

    public static final String SPOUT_TO_DIRECTOR_STREAM = "spout-to-director-stream";

    public static final String FRAME_ALGORITHM_STATUS_STREAM = "frame-algorithm-status-stream";
    public final static String FIELD_USE_SIFT = "field-use-sift";
    public final static String FIELD_USE_HOG = "field-use-hog";

    public static final String HOG_SAMPLE_FRAME_STREAM = "hog-sample-frame-stream";
    
    public static final String DETECTED_SIFT_SLM_LOGO_STREAM = "detected-sift-slm-logo-stream";
    public static final String FIELD_FOUND_SLM_RECT = "field-found-slm-rect";
    public static final String FIELD_FOUND_SLM_RECT_LIST = "field-found-slm-rect-list";

    public static final String FIELD_OUTPUT_RECTANGLES = "field-output-rectangles";

    public static final String DETECTED_HOG_SLM_LOGO_STREAM = "detected-hog-slm-logo-stream";
    
    public static final int SLM_LOGO_STATE_NORMAL = 0;
    public static final int SLM_LOGO_STATE_MUTED = 1;

    public static final String HOG_PATCH_FRAME_STREAM = "hog-patch-frame-stream";
    public static final String HOG_PROCESSED_FRAME_STREAM = "hog-processed-frame-stream";

////////////not used

    public static String FIELD_FEATURE_DESC = "feat";
    public static String FIELD_FEATURE_CNT = "feat-cnt";
    public static String STREAM_FEATURE_DESC = "feat-desc";
    public static String STREAM_FEATURE_COUNT = "feat-cnt";
    public static String FIELD_MATCH_IMAGES = "match-ids";
    public static String STREAM_MATCH_IMAGES = "matches";
    public static String CONF_FEAT_DIST_THRESHOLD = "vd.feature.dist.threshold";
    public static String CONF_MATCH_RATIO = "vd.match.min.ratio";
    public static String CONF_FEAT_PREFILTER_THRESHOLD = "vd.feature.prefilter.threshold";

}
