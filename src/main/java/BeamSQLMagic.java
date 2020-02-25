package com.google.cloud.zetasql;

//import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

import java.util.stream.Collectors;

/**
 * Do some randomness
 */
public class BeamSQLMagic {
    public static final String HEADER = "state_id,state_code,state_name,sales_region";
    public static final Schema SCHEMA = Schema.builder()
            .addInt64Field("state_id")
            .addStringField("state_code")
            .addStringField("state_name")
            .addStringField("sales_region")
            .build();

    public static void main(String[] args) {
        /*PipelineOptionsFactory.register(DataflowPipelineOptions.class);

        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        */
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        options.as(BeamSqlPipelineOptions.class).setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");
        
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("read_from_gcs", TextIO.read().from("gs://ruwang-test/cvs/test.csv"))
                .apply("transform_to_row", ParDo.of(new RowParDo())).setRowSchema(SCHEMA)
                .apply("transform_sql", SqlTransform.query(
                        "SELECT sales_region, COUNT(state_id) as num_state " +
                                "FROM PCOLLECTION GROUP BY sales_region ORDER BY 2 DESC LIMIT 20")
                )
                .apply("transform_to_string", ParDo.of(new RowToString()))
                .apply("write_to_gcs", TextIO.write().to("gs://ruwang-test/test_output/output.csv").withoutSharding());
        pipeline.run();
    }

    //ParDo for String -> Row (SQL)
    public static class RowParDo extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().equalsIgnoreCase(HEADER)) {
                String[] vals = c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                Row appRow = Row
                        .withSchema(SCHEMA)
                        .addValues(vals[4], Long.valueOf(vals[6]))
                        .build();
                c.output(appRow);
            }
        }
    }

    //ParDo for Row (SQL) -> String
    public static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String line = c.element().getValues()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            c.output(line);
        }
    }
}