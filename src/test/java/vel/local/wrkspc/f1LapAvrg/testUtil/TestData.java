package vel.local.wrkspc.f1LapAvrg.testUtil;

import org.apache.beam.sdk.values.KV;
import vel.local.wrkspc.f1LapAvrg.model.DriverLapRecord;

import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class TestData {

    public static final List<String> INPUT_CSV_RECORDS = Arrays.asList(
            "Alonzo,4.32",
            "Blonzo,4.32",
            "Verstrappen,4.75",
            "Terstrappen,4.75",
            "Alonzo,4.88",
            "Blonzo,4.88",
            "Hamilton,4.65",
            "Alonzo,4.38",
            "Blonzo,4.38",
            "Verstrappen,4.55",
            "Terstrappen,4.55",
            "Hamilton,4.61",
            "Hamilton,4.43",
            "Verstrappen,4.59",
            "Terstrappen,4.59",
            "TestDriver_4,2.23",
            "TestDriver_5,1.59",
            "TestDriver_4,2.54",
            "TestDriver_5,2.55"
    );

    public static final List<KV<String, Double>> PARSED_KEY_VALUES_TESTDATA = TestData.INPUT_CSV_RECORDS.stream().map(csvRecord -> {
        String[] split = csvRecord.split("[,]");
        return KV.of(split[0], Double.parseDouble(split[1]));
    }).collect(Collectors.toList());

    public static final List<KV<String, Double>> KEY_VALUES_DRIVER_AVERAGE = Arrays.asList(
            KV.of("Alonzo", 4.52666666666666666666),
            KV.of("Blonzo", 4.52666666666666666666),
            KV.of("Verstrappen", 4.63),
            KV.of("Terstrappen", 4.63),
            KV.of("Hamilton", 4.5633333333333333333),
            KV.of("TestDriver_4", 2.385),
            KV.of("TestDriver_5", 2.07));

    public static final SortedSet<DriverLapRecord> SORTED_DRIVERS = new TreeSet<>(KEY_VALUES_DRIVER_AVERAGE.stream()
            .map(kv -> new DriverLapRecord(kv.getKey(), kv.getValue())).collect(Collectors.toList()));

    public static final String FORMATTED_OUTPUT = SORTED_DRIVERS.stream()
            .map(value -> String.format("%s,%s", value.getName(), value.getLapTime()))
            .collect(Collectors.joining(System.lineSeparator()));

    public static final String UNSORTED_FORMATTED_OUTPUT = KEY_VALUES_DRIVER_AVERAGE.stream()
            .map(value -> String.format("%s,%s", value.getKey(), value.getValue()))
            .collect(Collectors.joining(System.lineSeparator()));


    public static final List<String> INPUT_ERR_CSV_RECORDS = Arrays.asList(
            "Alonzo,4.32,hafdasf",
            "Blonzo,4.32",
            "Verstrappen,4.75"
    );
    public static final List<String> INPUT_CSV_RECORDS_NUM_ERR = Arrays.asList(
            "Alonzo,safds",
            "Blonzo,4.32",
            "Verstrappen,4.75"
    );

}
