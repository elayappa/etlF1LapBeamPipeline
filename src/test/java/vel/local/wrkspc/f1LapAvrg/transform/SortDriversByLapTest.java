package vel.local.wrkspc.f1LapAvrg.transform;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import vel.local.wrkspc.f1LapAvrg.extract.ParseDriverLapCsvRecord;
import vel.local.wrkspc.f1LapAvrg.extract.ParseDriverLapCsvRecordTest;
import vel.local.wrkspc.f1LapAvrg.model.DriverLapRecord;
import vel.local.wrkspc.f1LapAvrg.testUtil.TestData;

import java.io.Serializable;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SortDriversByLapTest implements Serializable {

    @Rule
    public transient TestPipeline p = TestPipeline.create();

    @Test
    public void testSortDriversByLapTest() {
        PCollection<KV<String, Double>> parsedInputRecords =
                p.apply(Create.of(TestData.INPUT_CSV_RECORDS).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        PCollection<KV<String, Double>> driverLapAverage = parsedInputRecords.apply(new CalculateLapAverage());
        //PCollection<TreeSet<DriverLapRecord>> sortedDrivers = driverLapAverage.apply(new SortDriversByLap());
        PCollection<Iterable<KV<DriverLapRecord, Double>>> sortedDrivers = driverLapAverage.apply(new SortDriversByLap());
        PCollection<String> formatResults = sortedDrivers.apply("FormatResults", MapElements.via(new SimpleFunction<Iterable<KV<DriverLapRecord, Double>>, String>() {
            @Override
            public String apply(Iterable<KV<DriverLapRecord, Double>> input) {
                TreeSet<DriverLapRecord> driverLapRecords = new TreeSet<>(StreamSupport.stream(input.spliterator(), false)
                        .map(value -> value.getKey()).collect(Collectors.toList()));
                String collect = driverLapRecords.stream()
                        .map(value -> String.format("%s,%s", value.getName(), value.getLapTime()))
                        .collect(Collectors.joining(String.format(";")));
                return collect;
                /*return input.stream()
                        .map(value -> String.format("%s,%s", value.getName(), value.getLapTime()))
                        .collect(Collectors.joining(String.format(";")));*/
            }
        }));
        PAssert.that(formatResults).containsInAnyOrder(Arrays.asList(TestData.FORMATTED_OUTPUT));
        p.run().waitUntilFinish();
    }
}