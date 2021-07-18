package vel.local.wrkspc.f1LapAvrg.transform;

import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import vel.local.wrkspc.f1LapAvrg.model.DriverLapRecord;

import java.util.Arrays;
import java.util.TreeSet;

//public class SortDriversByLap extends PTransform<PCollection<KV<String, Double>>, PCollection<TreeSet<DriverLapRecord>>> {
public class SortDriversByLap extends PTransform<PCollection<KV<String, Double>>, PCollection<Iterable<KV<DriverLapRecord, Double>>>> {

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
    //public PCollection<TreeSet<DriverLapRecord>> expand(PCollection<KV<String, Double>> input) {
    public PCollection<Iterable<KV<DriverLapRecord, Double>>> expand(PCollection<KV<String, Double>> input) {
        TestPipeline p = TestPipeline.create();
        PCollection<TreeSet<DriverLapRecord>> treeSetOfDrivers = p.apply(Create.of(Arrays.asList(
                new TreeSet<DriverLapRecord>() {{
                    {
                        add(new DriverLapRecord("a", 1));
                        add(new DriverLapRecord("b", 2));
                    }
                }})));
        BufferedExternalSorter.Options options1 = BufferedExternalSorter.options();
        TreeSet<DriverLapRecord> driverLapRecordsTreeSet = new TreeSet<>();
        return input
                .apply("CreateKey", MapElements.via(new SimpleFunction<KV<String, Double>, KV<String, KV<DriverLapRecord, Double>>>() {
                    public KV<String, KV<DriverLapRecord, Double>> apply(KV<String, Double> input) {
                        return KV.of("sort", KV.of(new DriverLapRecord(input.getKey(), input.getValue()), input.getValue()));
                    }
                }))
                .apply(GroupByKey.create()).setCoder(KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(KvCoder.of(AvroCoder.of(DriverLapRecord.class), DoubleCoder.of()))))
                .apply(SortValues.create(options1))
                //.apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Iterable<KV<DriverLapRecord, Double>>>, TreeSet<DriverLapRecord>>() {
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Iterable<KV<DriverLapRecord, Double>>>, Iterable<KV<DriverLapRecord, Double>>>() {
                    @Override
                    //public TreeSet<DriverLapRecord> apply(KV<String, Iterable<KV<DriverLapRecord, Double>>> input) {
                    public Iterable<KV<DriverLapRecord, Double>> apply(KV<String, Iterable<KV<DriverLapRecord, Double>>> input) {
                        /*List<DriverLapRecord> driverLapRecords = StreamSupport.stream(input.getValue().spliterator(), false)
                                .map(value -> value.getKey())
                                .collect(Collectors.toList());
                        Collections.sort(driverLapRecords);
                        driverLapRecordsTreeSet.addAll(driverLapRecords);
                        return driverLapRecordsTreeSet;*/
                        return input.getValue();
                    }
                }));//.setCoder(treeSetOfDrivers.getCoder());

    }
}
