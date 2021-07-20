# etlF1LapBeamPipeline

### PreRequisites

Java and Gradle needs to be installed.

### Before Running the Pipeline:

The Pipeline is not yet enabled to watch for folders and pick the files on the fly as they are dropped.

So Please Drop the input CSV in the desired path, lets say \desiredInputCsvPath

If no path is desired then the default input path will be ./f1LapFeeds from the execution root path.

The default output path will be ./f1LapFeedsOut from the execution root path.

Both the default paths can be overridden by passing the VM execution argumets to pipeline as below
--inputPath=/desiredInput and --outputPath=/desiredOutputPath

### Running the Pipeline:

After installing the git project to local please execute the below commad from the installation root

__gradlew clean execute -DmainClass=vel.local.wrkspc.f1LapAvrg.EtlPipelineApplication -Pdirect-runner__

The above execution will use the default input file with the project *f1LapFeeds/f1LapFeed.csv*

And the ouput will get created in the path *\f1LapFeedsOut* under the installation root. The ouput filename will be in
the pattern *lapOutput-nnnnn-of-nnnnn*

If need to override the default Input and output paths, the below command can be executed. But Please make sure to drop
the input CSV file in desired input location.

Based on below command We need to drop the input CSV under D:\f1LapFeedsIn and then execute the below command

__gradlew clean execute -DmainClass=vel.local.wrkspc.f1LapAvrg.EtlPipelineApplication -Dexec.args="--inputPath=*D:
\f1LapFeedsIn* --outputPath=*D:\f1LapFeedsOut*" -Pdirect-runner__

Then We will see the output under the path *D:\f1LapFeedsOut*



