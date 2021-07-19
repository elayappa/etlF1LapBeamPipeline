package vel.local.wrkspc.f1LapAvrg.model;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface EtlF1LapPipelineOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path to read from")
    @Default.String("./f1LapFeeds")
    String getInputPath();

    void setInputPath(String value);

    /**
     * Set this required option to specify where to write the output.
     */
    @Description("Path to write to")
    @Validation.Required
    @Default.String("./f1LapFeedsOut")
    String getOutputPath();

    void setOutputPath(String value);
}
