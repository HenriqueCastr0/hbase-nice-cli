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
    private static HBaseAdmin admin;
    private static Configuration conf = new Configuration();

    private static final String LIST = "list";
    private static final String SCAN = "scan";
    private static final String COUNT = "count";
    private static final String TABLE_NAMES_FOLDER = System.getProperty("autocompletion.info.path");

    public HBaseLayer() throws IOException {
        conf.setQuietMode(true);
    }


    private ClassLoader getClassLoader(String jarPath) throws MalformedURLException {
        URL jarUrl = new File(jarPath).toURI().toURL();
        return URLClassLoader.newInstance(new URL[]{ jarUrl}, getClass().getClassLoader());
    }

    private Filter getFilter(@Nonnull String jarPath, @Nonnull String classFullName){
        ClassLoader loader = null;
        try {
            loader = getClassLoader(jarPath);
            Class<?> clazz = Class.forName(classFullName, true, loader);
            Class<? extends Filter> clazzRowFormat = clazz.asSubclass(Filter.class);
            Constructor<? extends Filter> ctor = clazzRowFormat.getConstructor();
            return ctor.newInstance();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     *
     * @param jarPath
     * @param classFullName fully qualified name of the desired class
     * @return
     */
    private RowFormat getRowFormat(@Nullable String jarPath, @Nullable String classFullName){
        if(jarPath==null || classFullName==null){
            return new DefaultRowFormat();
        }
        try {
            ClassLoader loader = getClassLoader(jarPath);
            Class<?> clazz = Class.forName(classFullName, true, loader);
            Class<? extends RowFormat> clazzRowFormat = clazz.asSubclass(RowFormat.class);
            Constructor<? extends RowFormat> ctor = clazzRowFormat.getConstructor();
            RowFormat rowFormatInstance = ctor.newInstance();
            return rowFormatInstance;
        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static void addGeneralOptions(Options options){
        Options generalOptions = new Options();
        generalOptions.addOption("q", "quorum", true, "HBase quorum");
        for(Object generalOption : generalOptions.getOptions()){
            options.addOption((Option) generalOption);
        }
    }

    public static void main(String[] args) throws Exception {
        // options for list
        Options listOptions = new Options();
        listOptions.addOption("l", "limit", true, "Maximum number of tables to return");
        listOptions.addOption("s", "sort", true, "Sort the tables");
        addGeneralOptions(listOptions);

        // options for scan
        Options scanOptions = new Options();
        Option tableOption = new Option("t", "table", true, "Table to scan");
        tableOption.setRequired(true);
        scanOptions.addOption(tableOption);
        scanOptions.addOption("l", "limit", true, "Maximum number of rows");
        scanOptions.addOption("cf", "columnFamily", true, "Set column family");
        scanOptions.addOption("j", "jarFile", true, "Jar file location");
        scanOptions.addOption("ff", "fileFormat", true, "Use file to format output");
        scanOptions.addOption("fi", "filter", true, "Use filter");
        addGeneralOptions(scanOptions);

        // options for count
        Options countOptions = new Options();
        countOptions.addOption(tableOption);
        addGeneralOptions(countOptions);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd;

        switch(args[0]){
            case LIST: {
                try {
                    cmd = parser.parse(listOptions, args);
                    parseGeneralOptions(cmd);
                    Integer limit = Integer.MAX_VALUE;
                    Sort sort = Sort.NONE;
                    if(cmd.hasOption("l")){
                        limit = Integer.valueOf(cmd.getOptionValue("l"));
                    }
                    if(cmd.hasOption("s")){
                        sort = Sort.valueOf(cmd.getOptionValue("s").toUpperCase());
                    }
                    new HBaseLayer().list(limit, sort);
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                }
            }
            break;
            case SCAN: {
                try {
                    cmd = parser.parse(scanOptions, args);
                    parseGeneralOptions(cmd);
                    String tableName = null;
                    Long limit = Long.MAX_VALUE;
                    String columnFamily = null;
                    String jarPath = null;
                    String formatClassName = null;
                    String filterClassName = null;
                    if(cmd.hasOption("t")){
                        tableName = cmd.getOptionValue("t");
                    }
                    if(cmd.hasOption("l")){
                        limit = Long.valueOf(cmd.getOptionValue("l"));
                    }
                    if(cmd.hasOption("cf")){
                        columnFamily = cmd.getOptionValue("cf");
                    }
                    if(cmd.hasOption("ff")){
                        formatClassName = cmd.getOptionValue("ff");
                    }
                    if(cmd.hasOption("j")){
                        jarPath = cmd.getOptionValue("j");
                    }
                    if(cmd.hasOption("fi")){
                        filterClassName = cmd.getOptionValue("fi");
                    }
                    new HBaseLayer().scan(tableName, limit, columnFamily, jarPath, formatClassName, filterClassName);
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                }
            }
            break;
            case COUNT: {
                try {
                    cmd = parser.parse(countOptions, args);
                    parseGeneralOptions(cmd);
                    String tableName = null;
                    if(cmd.hasOption("t")){
                        tableName = cmd.getOptionValue("t");
                    }
                    new HBaseLayer().count(tableName);
                } catch (ParseException e) {
                    System.out.println(e.getMessage());
                }
            }
            break;
        }

    }

    private static void parseGeneralOptions(CommandLine cmd) throws MasterNotRunningException, ZooKeeperConnectionException {
        if (cmd.hasOption("q")) {
            String quorum = cmd.getOptionValue("q");
            conf.set(HBASE_ZOOKEEPER_QUORUM, quorum);
        }
        admin = new HBaseAdmin(conf);
    }

    private void disable_all(){
//        listtables.foreach(t => admin.disableTable(t.getNameAsString))
    }

    private void drop_all(){
//        listtables.foreach(t => admin.deleteTable(t.getNameAsString))
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
            throws IOException {
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

