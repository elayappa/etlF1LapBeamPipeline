package vel.local.wrkspc.f1LapAvrg.extract;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class ParseDriverLapCsvRecordTest {

    static final List<String> inputCsvRecords = Arrays.asList(
            "Alonzo,4.32",
            "Verstrappen,4.75",
            "Alonzo,4.88",
            "Hamilton,4.65",
            "Alonzo,4.38",
            "Verstrappen,4.55",
            "Hamilton,4.61",
            "Hamilton,4.43",
            "Verstrappen,4.59",
            "TestDriver_4,2.23",
            "TestDriver_5,1.59",
            "TestDriver_4,2.54",
            "TestDriver_5,2.55"
    );

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    public void testParseDriverLapCsvRecords() {
        TestPipeline p = TestPipeline.create();
        PCollection<KV<String, Double>> output =
                p.apply(Create.of(inputCsvRecords).withCoder(StringUtf8Coder.of()))
                        .apply(ParDo.of(new ParseDriverLapCsvRecord()));
        PAssert.that(output).containsInAnyOrder(KV.of("Alonzo", 4.32),
                KV.of("Verstrappen", 4.75),
                KV.of("Alonzo", 4.88),
                KV.of("Hamilton", 4.65),
                KV.of("Alonzo", 4.38),
                KV.of("Verstrappen", 4.55),
                KV.of("Hamilton", 4.61),
                KV.of("Hamilton", 4.43),
                KV.of("Verstrappen", 4.59),
                KV.of("TestDriver_4", 2.23),
                KV.of("TestDriver_5", 1.59),
                KV.of("TestDriver_4", 2.54),
                KV.of("TestDriver_5", 2.55));
        p.run().waitUntilFinish();
    }
}