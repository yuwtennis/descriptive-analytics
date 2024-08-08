package org.example.dags.realestate;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.App;
import org.example.dags.Dag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Year;
import java.util.ArrayList;
import java.util.List;

public class RealEstateDag implements Dag {
    static Logger LOG = LoggerFactory.getLogger(RealEstateDag.class);

    public void process(Pipeline p) {

        final List<String> urls = createUrls(
                p.getOptions().as(App.DagOptions.class).getBacktrackedYears());

        LOG.info("Start running {}", RealEstateDag.class.getSimpleName());

        // 1. Start by getting the actual url provided by the server
        PCollection<String> dlUrls = p.apply(Create.of(urls))
                .apply(new GetDlUrlVertices.DownloadUrl());

        // 2. Next get the zip contents
        PCollection<RealEstatesXactRec> xacts =  dlUrls.apply(new ExtractZipContentsVertices.Extract());

        // 3. Insert into Bigquery using Bigquery Storage API
        xacts.apply(
                MapElements
                        .into(TypeDescriptor.of(TableRow.class))
                        .via((RealEstatesXactRec e) -> {
                            assert e != null;
                            return e.toTableRow();
                        }))
                .apply(BigQueryIO
                        .writeTableRows()
                        .to(RealEstateEnv.FULLY_QUALIFIED_TABLE_NAME)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withMethod(BigQueryIO.Write.Method.DEFAULT)
                );

        p.run().waitUntilFinish();
    }

    private List<String> createUrls(int backtrackedYears) {
        List<String> urls = new ArrayList<>();

        for (int i = 1; i <= backtrackedYears; i++) {
            int backtracked = Year.now().minusYears(i).getValue();
            urls.add(String.format(
            "https://www.reinfolib.mlit.go.jp/in-api/api-aur/aur/csv/transactionPrices?language=ja&areaCondition=address&prefecture=13&transactionPrice=true&closedPrice=true&kind=residential&seasonFrom=%d1&seasonTo=%d4",
                    backtracked, backtracked));
        }

        return urls;
    }
}
