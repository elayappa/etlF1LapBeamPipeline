package vel.local.wrkspc.f1LapAvrg;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import vel.local.wrkspc.f1LapAvrg.extract.ParseDriverLapCsvRecord;
import vel.local.wrkspc.f1LapAvrg.model.EtlF1LapPipelineOptions;
import vel.local.wrkspc.f1LapAvrg.transform.CalculateLapAverage;
import vel.local.wrkspc.f1LapAvrg.transform.SortDriversByLap;

import java.io.File;

public class EtlPipelineApplication {
    public static void main(String[] args) {
        EtlF1LapPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(EtlF1LapPipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        p.apply("ReadLines", TextIO.read().from(options.getInputPath() + File.separator + "*.csv"))
                .apply(ParDo.of(new ParseDriverLapCsvRecord()))
                .apply(new CalculateLapAverage())
                .apply(new SortDriversByLap())
                .apply(TextIO.write().to(options.getOutputPath() + File.separator + "lapOutput"));
        p.run().waitUntilFinish();
    }
}
