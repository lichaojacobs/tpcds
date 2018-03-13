package edu.brown.cs.systems.tpcds.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/** Settings for a TPC-DS dataset */
public class TPCDSSettings {

	public int scaleFactor;
	public String dataLocation;
	public String dataFormat;
	public boolean overwrite;
	public boolean useDoubleForDecimal;
	public boolean partitionTables;
	public boolean clusterByPartitionColumns;
	public boolean filterOutNullPartitionValues;

	private TPCDSSettings(Config config) {
		scaleFactor = config.getInt("scaleFactor");
		dataLocation = config.getString("dataLocation");
		dataFormat = config.getString("dataFormat");
		overwrite = config.getBoolean("overwrite");
		useDoubleForDecimal = config.getBoolean("useDoubleForDecimal");
		partitionTables = config.getBoolean("partitionTables");
		clusterByPartitionColumns = config.getBoolean("clusterByPartitionColumns");
		filterOutNullPartitionValues = config.getBoolean("filterOutNullPartitionValues");
	}

	public void setScaleFactor(int scaleFactor) {
		this.scaleFactor = scaleFactor;
	}

	public void setDataLocation(String dataLocation) {
		this.dataLocation = dataLocation;
	}

	public void setDataFormat(String dataFormat) {
		this.dataFormat = dataFormat;
	}

	public void setPartitionTables(boolean partitionTables) {
		this.partitionTables = partitionTables;
	}

	/**
	 * Create TPC-DS settings, taking values from the default typesafe config.
	 * This call is equivalent to
	 * {@code createFromConfig(ConfigFactory.load().getConfig("tpcds"))}
	 */
	public static TPCDSSettings createWithDefaults() {
		return createFromConfig(ConfigFactory.load().getConfig("tpcds"));
	}

	/**
	 * Create TPC-DS settings, taking values from the provided typesafe config
	 * object. The default values are contained in the "tpcds" root config
	 */
	public static TPCDSSettings createFromConfig(Config config) {
		return new TPCDSSettings(config);
	}
	
	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		b.append("scaleFactor: " + scaleFactor);
		b.append(" dataLocation: " + dataLocation);
		b.append(" dataFormat: " + dataFormat);
		b.append(" overwrite: " + overwrite);
		b.append(" useDoubleForDecimal: " + useDoubleForDecimal);
		b.append(" partitionTables: " + partitionTables);
		b.append(" clusterByPartitionColumns: " + clusterByPartitionColumns);
		b.append(" filterOutNullPartitionValues: " + filterOutNullPartitionValues);
		return b.toString();
	}
}
