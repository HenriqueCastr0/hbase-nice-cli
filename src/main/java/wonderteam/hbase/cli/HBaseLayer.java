package wonderteam.hbase.cli;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;

public class HBaseLayer {
    private final static String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String LIST = "list";
    private static final String SCAN = "scan";
    private static final String COUNT = "count";
    private static final String TABLE_NAMES_FOLDER = System.getProperty("autocompletion.info.path");
    private static final String OPTION_QUOROM = "q";
    private static final String OPTION_QUOROM_LONG = "quorum";
    private static final String OPTION_LIMIT = "l";
    private static final String OPTION_LIMIT_LONG = "limit";
    private static final String OPTION_SORT = "s";
    private static final String OPTION_SORT_LONG = "sort";
    private static final String OPTION_TABLE = "t";
    private static final String OPTION_TABLE_LONG = "table";
    private static final String OPTION_COLUMN_FAMILY = "cf";
    private static final String OPTION_COLUMN_FAMILY_LONG = "columnFamily";
    private static final String OPTION_FILE_FORMAT = "ff";
    private static final String OPTION_FILE_FORMAT_LONG = "fileFormat";
    private static final String OPTION_JAR_FILE = "j";
    private static final String OPTION_JAR_FILE_LONG = "jarFile";
    private static final String OPTION_FILTER = "fi";
    private static final String OPTION_FILTER_LONG = "filter";

    private static HBaseAdmin admin;
    private static Configuration conf = new Configuration();




    public static void main(String[] args) {
        CommandLineParser parser = new BasicParser();
        CommandLine cmd;
        try {
            switch (args[0]) {
                case LIST: {
                    cmd = parser.parse(getListOptions(), args);
                    parseGeneralOptions(cmd);
                    Integer limit = Integer.MAX_VALUE;
                    Sort sort = Sort.NONE;
                    if (cmd.hasOption(OPTION_LIMIT)) {
                        limit = Integer.valueOf(cmd.getOptionValue(OPTION_LIMIT));
                    }
                    if (cmd.hasOption(OPTION_SORT)) {
                        sort = Sort.valueOf(cmd.getOptionValue(OPTION_SORT).toUpperCase());
                    }
                    new HBaseLayer().list(limit, sort);
                }
                break;
                case SCAN: {
                    cmd = parser.parse(getScanOptions(), args);
                    parseGeneralOptions(cmd);
                    String tableName = null;
                    Long limit = Long.MAX_VALUE;
                    String columnFamily = null;
                    String jarPath = null;
                    String formatClassName = null;
                    String filterClassName = null;
                    if (cmd.hasOption(OPTION_TABLE)) {
                        tableName = cmd.getOptionValue(OPTION_TABLE);
                    }
                    if (cmd.hasOption(OPTION_LIMIT)) {
                        limit = Long.valueOf(cmd.getOptionValue(OPTION_LIMIT));
                    }
                    if (cmd.hasOption(OPTION_COLUMN_FAMILY)) {
                        columnFamily = cmd.getOptionValue(OPTION_COLUMN_FAMILY);
                    }
                    if (cmd.hasOption(OPTION_FILE_FORMAT)) {
                        formatClassName = cmd.getOptionValue(OPTION_FILE_FORMAT);
                    }
                    if (cmd.hasOption(OPTION_JAR_FILE)) {
                        jarPath = cmd.getOptionValue(OPTION_JAR_FILE);
                    }
                    if (cmd.hasOption(OPTION_FILTER)) {
                        filterClassName = cmd.getOptionValue(OPTION_FILTER);
                    }
                    new HBaseLayer().scan(tableName, limit, columnFamily, jarPath, formatClassName, filterClassName);
                }
                break;
                case COUNT: {
                    cmd = parser.parse(getCountOptions(), args);
                    parseGeneralOptions(cmd);
                    String tableName = null;
                    if (cmd.hasOption(OPTION_TABLE)) {
                        tableName = cmd.getOptionValue(OPTION_TABLE);
                    }
                    new HBaseLayer().count(tableName);

                }
                break;
            }
        }
        catch(Exception e){
            System.err.println(e.getMessage());
        }

    }

    public HBaseLayer() throws IOException {
        conf.setQuietMode(true);
    }

    private ClassLoader getClassLoader(String jarPath) throws MalformedURLException {
        URL jarUrl = new File(jarPath).toURI().toURL();
        return URLClassLoader.newInstance(new URL[]{ jarUrl}, getClass().getClassLoader());
    }

    private Filter getFilter(@Nonnull String jarPath, @Nonnull String classFullName)
            throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        ClassLoader loader = getClassLoader(jarPath);
        Class<?> clazz = Class.forName(classFullName, true, loader);
        Class<? extends Filter> clazzRowFormat = clazz.asSubclass(Filter.class);
        Constructor<? extends Filter> ctor = clazzRowFormat.getConstructor();
        return ctor.newInstance();
    }

    /**
     *
     * @param jarPath
     * @param classFullName fully qualified name of the desired class
     * @return
     */
    private RowFormat getRowFormat(@Nullable String jarPath, @Nullable String classFullName)
            throws MalformedURLException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException {
        if(jarPath==null || classFullName==null){
            return new DefaultRowFormat();
        }
        ClassLoader loader = getClassLoader(jarPath);
        Class<?> clazz = Class.forName(classFullName, true, loader);
        Class<? extends RowFormat> clazzRowFormat = clazz.asSubclass(RowFormat.class);
        Constructor<? extends RowFormat> ctor = clazzRowFormat.getConstructor();
        RowFormat rowFormatInstance = ctor.newInstance();
        return rowFormatInstance;
    }

    private static void addGeneralOptions(Options options){
        Options generalOptions = new Options();
        generalOptions.addOption(OPTION_QUOROM, OPTION_QUOROM_LONG, true, "HBase quorum");
        for(Object generalOption : generalOptions.getOptions()){
            options.addOption((Option) generalOption);
        }
    }

    private static Options getScanOptions(){
        Options scanOptions = new Options();
        scanOptions.addOption(getTableOption());
        scanOptions.addOption(OPTION_LIMIT, OPTION_LIMIT_LONG, true, "Maximum number of rows");
        scanOptions.addOption(OPTION_COLUMN_FAMILY, OPTION_COLUMN_FAMILY_LONG, true, "Set column family");
        scanOptions.addOption(OPTION_JAR_FILE, OPTION_JAR_FILE_LONG, true, "Jar file location");
        scanOptions.addOption(OPTION_FILE_FORMAT, OPTION_FILE_FORMAT_LONG, true, "Use file to format output");
        scanOptions.addOption(OPTION_FILTER, OPTION_FILTER_LONG, true, "Use filter");
        addGeneralOptions(scanOptions);
        return scanOptions;
    }

    private static Options getCountOptions(){
        Options countOptions = new Options();
        countOptions.addOption(getTableOption());
        addGeneralOptions(countOptions);
        return countOptions;
    }

    private static Options getListOptions(){
        Options listOptions = new Options();
        listOptions.addOption(OPTION_LIMIT, OPTION_LIMIT_LONG, true, "Maximum number of tables to return");
        listOptions.addOption(OPTION_SORT, OPTION_SORT_LONG, true, "Sort the tables");
        addGeneralOptions(listOptions);
        return listOptions;
    }

    private static void parseGeneralOptions(CommandLine cmd) throws MasterNotRunningException, ZooKeeperConnectionException {
        if (cmd.hasOption(OPTION_QUOROM)) {
            String quorum = cmd.getOptionValue(OPTION_QUOROM);
            conf.set(HBASE_ZOOKEEPER_QUORUM, quorum);
        }
        admin = new HBaseAdmin(conf);
    }

    private static Option getTableOption(){
        Option tableOption = new Option(OPTION_TABLE, OPTION_TABLE_LONG, true, "Table to scan");
        tableOption.setRequired(true);
        return tableOption;
    }


    private void disable_all(){
        // TODO
        // listtables.foreach(t => admin.disableTable(t.getNameAsString))
    }

    private void drop_all(){
        // TODO
        // listtables.foreach(t => admin.deleteTable(t.getNameAsString))
    }

    private void list(Integer limit, Sort sort) throws IOException {
        HTableDescriptor[] listTables = admin.listTables();
        List<HTableDescriptor> tables = Arrays.asList(listTables);
        Stream<HTableDescriptor> stream = tables.stream();

        stream = stream.limit(limit);
        switch(sort){
            case NONE:
                break;
            case ASC:
                stream = stream.sorted(new HTableDescriptorAscComparator());
                break;
            case DESC:
                stream = stream.sorted(new HTableDescriptorDescComparator());
                break;
        }
        final StringBuilder listOfTablesWithColumnFamilies = new StringBuilder();

        Consumer<HTableDescriptor> consumer = hTableDescriptor -> {
            System.out.println(hTableDescriptor.getNameAsString());
            listOfTablesWithColumnFamilies.append(hTableDescriptor.getNameAsString() + " ");
            HColumnDescriptor[] columnFamilies = hTableDescriptor.getColumnFamilies();
            if (columnFamilies.length > 0) {
                for (int i = 0; i < columnFamilies.length; i++) {
                    listOfTablesWithColumnFamilies.append(columnFamilies[i].getNameAsString());
                    if (i != columnFamilies.length - 1) {
                        listOfTablesWithColumnFamilies.append(" ");
                    }
                }
            }
            listOfTablesWithColumnFamilies.append("\n");
        };

        stream.forEach(consumer::accept);
        writeToAutoCompletionFileForTableNames(listOfTablesWithColumnFamilies.toString());
    }

    private void writeToAutoCompletionFileForTableNames(String tableNamesAndColumnFamilies) throws IOException {
        String quorum = conf.get(HBASE_ZOOKEEPER_QUORUM);
        File folder = new File(TABLE_NAMES_FOLDER);
        folder.mkdirs();
        File file = new File(TABLE_NAMES_FOLDER + "/", quorum + "-hbase-cli-table-names");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        fileOutputStream.write(tableNamesAndColumnFamilies.getBytes(StandardCharsets.UTF_8));
    }

    private void count(String tableName) throws Exception {
        RowCounter rowCounter = new RowCounter();
        Job rowCounterSubmittableJob = rowCounter.createSubmittableJob(conf, new String[]{tableName});
        rowCounterSubmittableJob.waitForCompletion(true);
        CounterGroup group1 =
                rowCounterSubmittableJob.getCounters().getGroup("org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters");
        Counter rows = group1.findCounter("ROWS");
        System.out.println(rows.getValue());
    }

    private void scan(String tableName, Long limit, @Nonnull String columnFamily, @Nullable String jarPath, @Nullable String formatClassName,
            @Nullable String filterClassName)
            throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        final RowFormat rowFormat = getRowFormat(jarPath, formatClassName);
        Scan scan = null;
        ResultScanner resultScanner;
        if(!admin.tableExists(tableName.getBytes())){
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        HTable hTable = new HTable(conf, tableName.getBytes());
        scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));

        if(jarPath != null && filterClassName != null){
            Filter filter = getFilter(jarPath, filterClassName);
            scan.setFilter(filter);
        }
        resultScanner  = hTable.getScanner(scan);
        Iterator<Result> iterator = resultScanner.iterator();
        Iterable<Result> iterable = () -> iterator;
        Stream<Result> stream = StreamSupport.stream(iterable.spliterator(), false);

        stream = stream.limit(limit);
        stream.forEach(result -> System.out.println(rowFormat.format(result, Collections.singletonList(columnFamily))));
    }

    public enum Sort{
        NONE, ASC, DESC;
    }

    public class HTableDescriptorAscComparator implements Comparator<HTableDescriptor>{

        @Override
        public int compare(HTableDescriptor o1, HTableDescriptor o2) {
            return o1.getNameAsString().compareTo(o2.getNameAsString());
        }
    }

    public class HTableDescriptorDescComparator implements Comparator<HTableDescriptor>{

        @Override
        public int compare(HTableDescriptor o1, HTableDescriptor o2) {
            return o2.getNameAsString().compareTo(o1.getNameAsString());
        }
    }
}

