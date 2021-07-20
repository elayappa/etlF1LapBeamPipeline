package vel.local.wrkspc.f1LapAvrg.transform;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import vel.local.wrkspc.f1LapAvrg.extract.ParseDriverLapCsvRecord;
import vel.local.wrkspc.f1LapAvrg.testUtil.TestData;

import java.io.Serializable;
import java.util.Arrays;

public class SortDriversByLapTest implements Serializable {

    @Rule
    public transient TestPipeline p = TestPipeline.create();

    @Test
    public void testSortDriversByLapTest() {
        PCollection<KV<String, Double>> parsedInputRecords =
                p.apply(Create.of(TestData.INPUT_CSV_RECORDS).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        PCollection<KV<String, Double>> driverLapAverage = parsedInputRecords.apply(new CalculateLapAverage());
        PCollection<String> sortedDrivers = driverLapAverage.apply(new SortDriversByLap());
        PAssert.that(sortedDrivers).containsInAnyOrder(Arrays.asList(TestData.FORMATTED_OUTPUT));
        p.run().waitUntilFinish();
    }

    @Test(expected = AssertionError.class)
    public void testSortDriversByLapTestFailure() {
        PCollection<KV<String, Double>> parsedInputRecords =
                p.apply(Create.of(TestData.INPUT_CSV_RECORDS).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        PCollection<KV<String, Double>> driverLapAverage = parsedInputRecords.apply(new CalculateLapAverage());
        PCollection<String> sortedDrivers = driverLapAverage.apply(new SortDriversByLap());
        PAssert.that(sortedDrivers).containsInAnyOrder(Arrays.asList(TestData.UNSORTED_FORMATTED_OUTPUT));
        p.run().waitUntilFinish();
    }
}