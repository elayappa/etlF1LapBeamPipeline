package vel.local.wrkspc.f1LapAvrg.transform;

import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import vel.local.wrkspc.f1LapAvrg.model.DriverLapRecord;

import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SortDriversByLap extends PTransform<PCollection<KV<String, Double>>, PCollection<String>> {

    /**
     * Override this method to specify how this {@code PTransform} should be expanded on the given
     * {@code InputT}.
     *
     * <p>NOTE: This method should not be called directly. Instead apply the {@code PTransform} should
     * be applied to the {@code InputT} using the {@code apply} method.
     *
     * <p>Composite transforms, which are defined in terms of other transforms, should return the
     * output of one of the composed transforms. Non-composite transforms, which do not apply any
     * transforms internally, should return a new unbound output and register evaluators (via
     * backend-specific registration methods).
     *
     * @param input
     * @return
     */
    @Override
    public PCollection<String> expand(PCollection<KV<String, Double>> input) {
        return input
                .apply("CreateKey", MapElements.via(new SimpleFunction<KV<String, Double>, KV<String, KV<DriverLapRecord, Double>>>() {
                    public KV<String, KV<DriverLapRecord, Double>> apply(KV<String, Double> input) {
                        return KV.of("sort", KV.of(new DriverLapRecord(input.getKey(), input.getValue()), input.getValue()));
                    }
                }))
                .apply(GroupByKey.create()).setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(KvCoder.of(AvroCoder.of(DriverLapRecord.class), DoubleCoder.of()))))
                .apply("SortAndFormatResults", MapElements.via(new SimpleFunction<KV<String, Iterable<KV<DriverLapRecord, Double>>>, String>() {
                    @Override
                    public String apply(KV<String, Iterable<KV<DriverLapRecord, Double>>> input) {
                        TreeSet<DriverLapRecord> driverLapRecords = new TreeSet<>(StreamSupport.stream(input.getValue().spliterator(), false)
                                .map(value -> value.getKey()).collect(Collectors.toList()));
                        String collect = driverLapRecords.stream()
                                .map(value -> String.format("%s,%s", value.getName(), value.getLapTime()))
                                .collect(Collectors.joining(System.lineSeparator()));
                        return collect;
                    }
                }));
    }
}
