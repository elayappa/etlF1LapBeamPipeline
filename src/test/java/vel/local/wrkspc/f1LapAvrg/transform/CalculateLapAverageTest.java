package vel.local.wrkspc.f1LapAvrg.transform;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import vel.local.wrkspc.f1LapAvrg.extract.ParseDriverLapCsvRecord;
import vel.local.wrkspc.f1LapAvrg.extract.ParseDriverLapCsvRecordTest;

public class CalculateLapAverageTest {

    public static final KV[] KEY_VALUES_DRIVER_AVERAGE = {
            KV.of("Alonzo", 4.526667),
            /*KV.of("Alonzo", 4.88),
            KV.of("Alonzo", 4.38),*/
            KV.of("Verstrappen", 4.63),
            /*KV.of("Verstrappen", 4.55),
            KV.of("Verstrappen", 4.59),*/
            KV.of("Hamilton", 4.5633333333333333333),
            /*KV.of("Hamilton", 4.61),
            KV.of("Hamilton", 4.43),*/
            KV.of("TestDriver_4", 2.385),
            /*KV.of("TestDriver_4", 2.54),*/
            KV.of("TestDriver_5", 2.07)/*,
            KV.of("TestDriver_5", 2.55)*/};

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testDriverlapAverage() {
        PCollection<KV<String, Double>> parsedInputRecords =
                p.apply(Create.of(ParseDriverLapCsvRecordTest.INPUT_CSV_RECORDS).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        PCollection<KV<String, Double>> driverLapAverage = parsedInputRecords.apply(new CalculateLapAverage());
        PAssert.that(driverLapAverage).containsInAnyOrder(KEY_VALUES_DRIVER_AVERAGE);
    }
}