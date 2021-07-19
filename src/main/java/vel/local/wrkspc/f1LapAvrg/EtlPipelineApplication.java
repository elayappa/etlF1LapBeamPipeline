package vel.local.wrkspc.f1LapAvrg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import vel.local.wrkspc.f1LapAvrg.extract.ParseDriverLapCsvRecord;
import vel.local.wrkspc.f1LapAvrg.transform.CalculateLapAverage;
import vel.local.wrkspc.f1LapAvrg.transform.SortDriversByLap;

public class EtlPipelineApplication {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);
        p.apply("ReadLines", TextIO.read().from("./f1LapFeeds/*.csv"))
                .apply(ParDo.of(new ParseDriverLapCsvRecord()))
                .apply(new CalculateLapAverage())
                .apply(new SortDriversByLap())
                .apply(TextIO.write().to("./f1LapFeedsOut/lapOutput"));
        p.run().waitUntilFinish();
    }
}
