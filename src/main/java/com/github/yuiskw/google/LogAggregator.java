package com.github.yuiskw.google;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * This class is used for wring log from Google Pub/Sub to BigQuery with Apache Beam.
 */
public class LogAggregator {

  /** command line options interface */
  public interface Options extends DataflowPipelineOptions {
    @Description("Output BigQuery dataset name")
    @Validation.Required
    String getOutputBigQueryDataset();
    void setOutputBigQueryDataset(String outputBigQueryDataset);

    @Description("Input Google Pub/Sub subscriptions name")
    @Validation.Required
    String getInputPubsubSubscription();
    void setInputPubsubSubscription(String inputPubsubSubscription);

    @Description("Output BigQuery table name prefix")
    @Validation.Required
    String getOutputBigQueryTable();
    void setOutputBigQueryTable(String outputBigQueryTable);
  }

  public static void main(String[] args) {
    Options options = getOptions(args);
    run(options);
  }

  protected static void run(Options options) {
    String projectId = options.getProject();
    String inputSubscription = options.getInputPubsubSubscription();
    String datasetId = options.getOutputBigQueryDataset();
    String tablePrefix = options.getOutputBigQueryTable();

    // Input
    String subscriptionName = "projects/" + projectId + "/subscriptions/" + inputSubscription;
    PubsubIO.Read<String> pubsubReader = PubsubIO.<String>read().subscription(subscriptionName)
        .withCoder(StringUtf8Coder.of());

    // Output
    TableSchema schema = PubsubMessage2TableRowFn.getOutputTableSchema();
    TableNameByWindowFn tableRefefenceFunction =
        new TableNameByWindowFn(projectId, datasetId, tablePrefix);
    BigQueryIO.Write.Bound bqWriter = BigQueryIO.Write
        .withSchema(schema)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        .to(tableRefefenceFunction);

    // Build and run pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(options.getInputPubsubSubscription(), pubsubReader)
        .apply(new LogTransformer())
        .apply(options.getOutputBigQueryTable(), bqWriter);
    pipeline.run();
  }

  protected static class LogTransformer
      extends PTransform<PCollection<String>, PCollection<TableRow>> {
    @Override
    public PCollection<TableRow> expand(PCollection<String> input) {
      return input
          .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(1))))
          .apply(ParDo.of(new PubsubMessage2TableRowFn()));
    }
  }

  protected static Options getOptions(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);
    return options;
  }
}
