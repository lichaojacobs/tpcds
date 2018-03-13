package edu.brown.cs.systems.tpcds.spark;

import org.apache.commons.cli.*;
import org.apache.spark.sql.SparkSession;

import com.databricks.spark.sql.perf.tpcds.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkTPCDSDataGenerator {

    public static final Logger log = LoggerFactory.getLogger(SparkTPCDSBatchGenerator.class);
    private static final CommandLineParser PARSER = new GnuParser();
    private static Options options = defineOptions();

    /** Generate data using the default dataset settings */
    public static void generateData() {
        generateData(TPCDSSettings.createWithDefaults());
    }

    /**
     * Generate data using some default dataset settings, overridding the specified values
     */
    public static void generateData(String location, String format, int scaleFactor) {
        TPCDSSettings settings = TPCDSSettings.createWithDefaults();
        settings.dataLocation = location;
        settings.dataFormat = format;
        settings.scaleFactor = scaleFactor;
        generateData(settings);
    }

    /** Generate data using the specified dataset settings */
    public static void generateData(TPCDSSettings settings) {
        SparkSession sparkSession = SparkSession.builder().appName("TPC-DS generateData").getOrCreate();
        Tables tables = new Tables(sparkSession.sqlContext(), settings.scaleFactor);
        tables.genData(settings.dataLocation, settings.dataFormat, settings.overwrite, settings.partitionTables,
                settings.useDoubleForDecimal, settings.clusterByPartitionColumns, settings.filterOutNullPartitionValues, "");

        sparkSession.sparkContext();
    }

    public static void main(String[] args) throws Exception {
        TPCDSSettings settings = TPCDSSettings.createWithDefaults();
        CommandLine commandLine = PARSER.parse(options, args);
        settings.setDataLocation(commandLine.getOptionValue("dataLocation", settings.dataLocation));
        settings.setScaleFactor(Integer.valueOf(commandLine.getOptionValue("scaleFactor", String.valueOf(settings.scaleFactor))));
        settings.setDataFormat(commandLine.getOptionValue("dataFormat", settings.dataFormat));
        settings.setPartitionTables(
                Boolean.valueOf(commandLine.getOptionValue("partitionTables", String.valueOf(settings.partitionTables))));
        log.info("Creating TPC-DS data using spark, with default settings:");
        log.info(settings.toString());
        generateData(settings);
    }

    private static Options defineOptions() {
        Options options = new Options();

        Option dataLocationOption = new Option("dataLocation", "dataLocation", true, "hdfs data location");
        dataLocationOption.setRequired(true);
        options.addOption(dataLocationOption);

        Option scaleFactorOption = new Option("scaleFactor", "scaleFactor", true, "scaleFactor");
        scaleFactorOption.setRequired(true);
        options.addOption(scaleFactorOption);

        Option dataFormatOption = new Option("dataFormat", "dataFormat", true, "dataFormat");
        dataFormatOption.setRequired(true);
        options.addOption(dataFormatOption);

        Option partitionTables = new Option("partitionTables", "partitionTables", true, "partitionTables");
        partitionTables.setRequired(true);
        options.addOption(partitionTables);

        return options;
    }
}
