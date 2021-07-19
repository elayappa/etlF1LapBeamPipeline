package vel.local.wrkspc.f1LapAvrg.extract;

import org.apache.beam.sdk.Pipeline;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import vel.local.wrkspc.f1LapAvrg.testUtil.TestData;

@RunWith(JUnit4.class)
public class ParseDriverLapCsvRecordTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testParseDriverLapCsvRecords() {
        PCollection<KV<String, Double>> output =
                p.apply(Create.of(TestData.INPUT_CSV_RECORDS).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        PAssert.that(output).containsInAnyOrder(TestData.PARSED_KEY_VALUES_TESTDATA);
        p.run().waitUntilFinish();
    }

    @Test(expected = Pipeline.PipelineExecutionException.class)
    public void testParseMalformedDriverLapCsvRecords() {
        PCollection<KV<String, Double>> output =
                p.apply(Create.of(TestData.INPUT_ERR_CSV_RECORDS).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        p.run().waitUntilFinish();
    }

    @Test(expected = Pipeline.PipelineExecutionException.class)
    public void testParseMalformedDriverLapTimeCsvRecords() {
        PCollection<KV<String, Double>> output =
                p.apply(Create.of(TestData.INPUT_CSV_RECORDS_NUM_ERR).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        p.run().waitUntilFinish();
    }
}