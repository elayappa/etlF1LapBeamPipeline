package vel.local.wrkspc.f1LapAvrg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class EtlPipelineApplication {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);
        p.apply(
                FileIO.match()
                        .filepattern("./f1LapFeeds/*.cs")
                        .continuously(
                                Duration.standardSeconds(30),
                                Watch.Growth.afterTimeSinceNewOutput(Duration.standardHours(1))));

        PCollection<String> lines = p.apply(
                "ReadMyFile", TextIO.read().from("gs://some/inputData.txt"));
    }
}
