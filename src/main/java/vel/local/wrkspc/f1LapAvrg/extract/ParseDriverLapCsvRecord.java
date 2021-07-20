package vel.local.wrkspc.f1LapAvrg.extract;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import vel.local.wrkspc.f1LapAvrg.exception.InvalidLapCsvRecordException;

public class ParseDriverLapCsvRecord extends DoFn<String, KV<String, Double>> {
    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<KV<String, Double>> parsedObjects) throws InvalidLapCsvRecordException {
        String[] split = line.split("[,]");
        if (split.length > 2) {
            throw new InvalidLapCsvRecordException("Record having more than 2 values in a row separated by comma. The record line follows " + line);
        }
        parsedObjects.output(KV.of(split[0], Double.parseDouble(split[1].trim())));
    }
}
