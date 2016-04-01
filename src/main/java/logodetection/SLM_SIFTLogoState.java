package logodetection;

import static tool.Constants.*;

/**
 * Created by Ian.
 *
 * This is a simple class to store the state for SIFT logos collected from SLM.
 * Logic manipulation is not done by this class, but rather done outside. This is simply
 * a collection of variables (i.e. just a tuple of values).
 */
public class SLM_SIFTLogoState {
    private StormVideoLogoDetectorGamma detector;
    private int lastStateUpdateId = 0;
    private int state = SLM_LOGO_STATE_NORMAL;

    public SLM_SIFTLogoState(StormVideoLogoDetectorGamma detector, int lastStateUpdateId, int state) {
        this.detector = detector;
        this.lastStateUpdateId = lastStateUpdateId;
        this.state = state;
    }

    public StormVideoLogoDetectorGamma getDetector() {
        return detector;
    }

    public void setDetector(StormVideoLogoDetectorGamma detector) {
        this.detector = detector;
    }

    public int getLastStateUpdateId() {
        return lastStateUpdateId;
    }

    public void setLastStateUpdateId(int lastStateUpdateId) {
        this.lastStateUpdateId = lastStateUpdateId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
