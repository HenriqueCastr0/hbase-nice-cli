package wonderteam.hbase.cli;

import java.util.List;
import org.apache.hadoop.hbase.client.Result;

public interface RowFormat {

    /**
     *
     * @param row A row returned by the scanner
     * @param columnFamilies The column families that were requested
     * @return The formatted row
     */
    public String format(Result row, List<String> columnFamilies);
}
