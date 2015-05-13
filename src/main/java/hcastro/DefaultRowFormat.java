package hcastro;

import java.util.NavigableMap;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DefaultRowFormat implements RowFormat {

    @Override
    public String format(Result row) {
        StringBuilder res = new StringBuilder();
        res.append("Key = " + Bytes.toStringBinary(row.getRow()) + "\n");
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = row.getMap();
        for(byte[] family : map.keySet()){
            NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get(family);
            for(byte[] qualifier : familyMap.keySet()){
                NavigableMap<Long, byte[]> valueMap = familyMap.get(qualifier);
                for(byte[] value : valueMap.values()){
                    res.append("Family = " + Bytes.toStringBinary(family) + ", Qualifier = " + Bytes.toStringBinary(qualifier) + ", " +
                            "Value = " + Bytes.toStringBinary(value) + "\n");
                }
            }
        }
        return res.toString();
    }
}
