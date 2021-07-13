package vel.local.wrkspc.f1LapAvrg.extract;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ParseDriverLapCsvRecord extends DoFn<String, KV<String, Double>> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<KV<String, Double>> parsedObjects) {
        String[] split = line.split("[,]");
        parsedObjects.output(KV.of(split[0], Double.parseDouble(split[1].trim())));
    }
}
