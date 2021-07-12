package vel.local.wrkspc.f1LapAvrg.extract;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ParseDriverLapCsvRecord extends DoFn<String, KV<String, Double>> {
}
