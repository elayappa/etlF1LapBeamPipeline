package vel.local.wrkspc.f1LapAvrg.transform;

import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class CalculateLapAverage extends PTransform<PCollection<KV<String, Double>>, PCollection<KV<String, Double>>> {

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
     */
    @Override
    public PCollection<KV<String, Double>> expand(PCollection<KV<String, Double>> input) {
        return null;
    }
}
