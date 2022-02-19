package rocksdbTuning;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

import java.util.Collection;

import static org.apache.flink.configuration.ConfigOptions.key;

public class StatisticConfigurableOptionsFactory extends DefaultConfigurableOptionsFactory {
    private static final long serialVersionUID = 1L;

    private String dbLogDir = "";
//    private Statistics statistics = new Statistics();

    public static final ConfigOption<String> LOG_DIR =
            key("state.backend.rocksdb.log.dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Location of RocksDB's info LOG file (empty = data dir, otherwise the " +
                            "data directory's absolute path will be used as the log file prefix)");
    @Override
    public DBOptions createDBOptions(DBOptions currentOptions,
                                     Collection<AutoCloseable> handlesToClose) {
        currentOptions = super.createDBOptions(currentOptions, handlesToClose);
        
        Statistics statistics = new Statistics();
        statistics.setStatsLevel(StatsLevel.ALL);
        currentOptions.setStatistics(statistics);
        currentOptions.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
        currentOptions.setStatsDumpPeriodSec(30);
        currentOptions.setDbLogDir(dbLogDir);
        DumpStat dumpStat = new DumpStat(statistics);
        dumpStat.start();

        return currentOptions;
    }

    @Override
    public String toString() {
        return this.getClass().toString() + "{" + super.toString() + '}';
    }

    /**
     * Set directory where RocksDB writes its info LOG file (empty = data dir, otherwise the
     * data directory's absolute path will be used as the log file prefix).
     */
    public void setDbLogDir(String dbLogDir) {
        this.dbLogDir = dbLogDir;
    }

    @Override
    public DefaultConfigurableOptionsFactory configure(Configuration configuration) {
        DefaultConfigurableOptionsFactory optionsFactory =
                super.configure(configuration);

        this.dbLogDir = configuration.getOptional(LOG_DIR).orElse(this.dbLogDir);

        return optionsFactory;
    }

    class DumpStat implements Runnable {
        private Thread t;
        private Statistics statistics;

        DumpStat(Statistics statistics) {
            this.statistics = statistics;
        }

        public void run() {
            try {
                while(true) {
                    Thread.sleep(20000);
                    System.out.println(System.currentTimeMillis() + "/n" + statistics.toString());
                }
            }catch (InterruptedException e) {
                System.out.println("Thread interrupted.");
            }
            System.out.println("Thread exiting.");
        }

        public void start() {
            System.out.println("Starting DumpStat");
            if (t == null) {
                t = new Thread (this, "DumpStat");
                t.start ();
            }
        }
    }

}
