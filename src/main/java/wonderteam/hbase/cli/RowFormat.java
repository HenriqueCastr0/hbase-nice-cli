package wonderteam.hbase.cli;

import org.apache.hadoop.hbase.client.Result;

public interface RowFormat {

    /**
     *
     * @param row A row returned by the scanner
     * @return The formatted row
     */
    public String format(Result row);
}
