package xyz.shiqihao.yarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class AppYarnClient {
    private Configuration conf;
    private YarnClient client;
    private Options opts;

    public AppYarnClient() {
        conf = new YarnConfiguration();
        client = YarnClient.createYarnClient();
        client.init(conf);
        opts = new Options();

        opts.addOption("appname", true, "Application Name");
        opts.addOption("jar", true, "JAR file containing the application");
    }

    public static void main(String[] args) {
        AppYarnClient client = new AppYarnClient();
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine line = parser.parse(client.opts, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("my app", client.opts);
    }
}
