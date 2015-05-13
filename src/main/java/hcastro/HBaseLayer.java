package hcastro;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.tools.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;

public class HBaseLayer {



    private static HBaseAdmin admin;
    private Map<String,Object> optionMap = new HashMap<String, Object>();
    private static Configuration conf = new Configuration();

    private static final String LIST = "list";
    private static final String SCAN = "scan";
    private static final String COUNT = "count";

    public HBaseLayer() throws IOException {
        conf.setQuietMode(true);
//        conf.set("hbase.zookeeper.quorum", "127.0.0.1");
    }

    public static void main(String[] args) throws Exception {
        // general options
        Options generalOptions = new Options();
        generalOptions.addOption("q", "quorum", true, "HBase quorum");

        // options for list
        Options listOptions = new Options();
        listOptions.addOption("l", "limit", true, "Maximum number of tables to return");
        listOptions.addOption("s", "sort", true, "Sort the tables");
        // add general options
        for(Object generalOption : generalOptions.getOptions()){
            listOptions.addOption((Option) generalOption);
        }

        // options for scan
        Options scanOptions = new Options();
        Option tableOption = new Option("t", "table", true, "Table to scan");
        tableOption.setRequired(true);
        scanOptions.addOption(tableOption);
        scanOptions.addOption("l", "limit", true, "Maximum number of rows");
        scanOptions.addOption("f", "fileFormat", true, "Use file to format output");
        scanOptions.addOption("c", "columnFamily", true, "Set column family");
        // add general options
        for(Object generalOption : generalOptions.getOptions()){
            scanOptions.addOption((Option) generalOption);
        }

        // options for count
        Options countOptions = new Options();
        countOptions.addOption(tableOption);
        // add general options
        for(Object generalOption : generalOptions.getOptions()){
            countOptions.addOption((Option) generalOption);
        }

        // parse the general options


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
                    RowFormat rowFormat = new DefaultRowFormat();
                    String columnFamily = null;
                    if(cmd.hasOption("t")){
                        tableName = cmd.getOptionValue("t");
                    }
                    if(cmd.hasOption("l")){
                        limit = Long.valueOf(cmd.getOptionValue("l"));
                    }
                    if(cmd.hasOption("f")){
                        String fileFormat = cmd.getOptionValue("f");
                        loadRowFormat(fileFormat);
                    }
                    if(cmd.hasOption("c")){
                        columnFamily = cmd.getOptionValue("c");
                    }
                    new HBaseLayer().scan(tableName, limit, rowFormat, columnFamily);
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

    private static RowFormat loadRowFormat(String fileFormat) {
            File rowFormatImplFile = new File(fileFormat);
            if (rowFormatImplFile.getParentFile().exists() || rowFormatImplFile.getParentFile().mkdirs()) {

                try {


                    /** Compilation Requirements *********************************************************************************************/
                    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
                    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
                    StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, null);

                    // This sets up the class path that the compiler will use.
                    // I've added the .jar file that contains the DoStuff interface within in it...
                    List<String> optionList = new ArrayList<String>();
                    optionList.add("-classpath");
                    optionList.add(System.getProperty("java.class.path") + ";dist/InlineCompiler.jar");

                    Iterable<? extends JavaFileObject> compilationUnit
                            = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(rowFormatImplFile));
                    JavaCompiler.CompilationTask task = compiler.getTask(
                            null,
                            fileManager,
                            diagnostics,
                            optionList,
                            null,
                            compilationUnit);
                    /********************************************************************************************* Compilation Requirements **/
                    if (task.call()) {
                        /** Load and execute *************************************************************************************************/
                        System.out.println("Yipe");
                        // Create a new custom class loader, pointing to the directory that contains the compiled
                        // classes, this should point to the top of the package structure!
                        URLClassLoader classLoader = new URLClassLoader(new URL[]{new File("./").toURI().toURL()});
                        // Load the class from the classloader by name....
                        Class<?> loadedClass = classLoader.loadClass(fileFormat);
                        // Create a new instance...
                        Object obj = loadedClass.newInstance();
                        // Santity check
                        if (obj instanceof RowFormat) {
                            // Cast to the DoStuff interface
                            RowFormat stuffToDo = (RowFormat)obj;
                            return stuffToDo;
                        }
                        /************************************************************************************************* Load and execute **/
                    } else {
                        for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                            System.out.format("Error on line %d in %s%n",
                                    diagnostic.getLineNumber(),
                                    diagnostic.getSource().toUri());
                        }
                    }
                    fileManager.close();
                } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException exp) {
                    exp.printStackTrace();
                }
            }
        return null;
    }

    private static void parseGeneralOptions(CommandLine cmd) throws MasterNotRunningException, ZooKeeperConnectionException {
        if (cmd.hasOption("q")) {
            String quorum = cmd.getOptionValue("q");
            conf.set("hbase.zookeeper.quorum", quorum);
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
        stream.forEach(table -> System.out.println(table.getNameAsString()));
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

    private void scan(String tableName, Long limit, RowFormat rowFormat, String columnFamily) throws IOException {
        Scan scan = null;
        ResultScanner resultScanner= null;
        if(!admin.tableExists(tableName.getBytes())){
            throw new IllegalArgumentException("Table " + tableName + " does not exist");
        }
        HTableDescriptor table = admin.getTableDescriptor(tableName.getBytes());
        HTable hTable = new HTable(conf, tableName.getBytes());
        scan = new Scan();
        if(columnFamily != null){
            scan.addFamily(Bytes.toBytes(columnFamily));
        }

        resultScanner  = hTable.getScanner(scan);
        Iterator<Result> iterator = resultScanner.iterator();
        Iterable<Result> iterable = () -> iterator;
        Stream<Result> stream = StreamSupport.stream(iterable.spliterator(), false);

        stream = stream.limit(limit);
        stream.forEach(result -> System.out.println(rowFormat.format(result)));
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

