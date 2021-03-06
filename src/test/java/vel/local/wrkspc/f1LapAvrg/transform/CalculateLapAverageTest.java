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
import vel.local.wrkspc.f1LapAvrg.testUtil.TestData;

public class CalculateLapAverageTest {

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
                p.apply(Create.of(TestData.INPUT_CSV_RECORDS).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        PCollection<KV<String, Double>> driverLapAverage = parsedInputRecords.apply(new CalculateLapAverage());
        PAssert.that(driverLapAverage).containsInAnyOrder(TestData.KEY_VALUES_DRIVER_AVERAGE);

        p.run().waitUntilFinish();
    }
}