package spout;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class EMTSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 2248085712685166977L;
	
	private static String EMT_URL = "https://openbus.emtmadrid.es:9443/emt-proxy-server/last/";
	private static String API_KEY = "idClient=EMT.SERVICIOS.EMTJSONBETA&passKey=4F4EEB75-A822-41E3-817B-AB301D5DA321";
	
	private static String GET_CALENDAR = "bus/GetCalendar";
	private static String GET_STOPS = "bus/GetNodesLines";
	
	private URL url;
	private SpoutOutputCollector collector;
	private int timeWindowInSeconds;

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		try {
			this.url = new URL(EMT_URL);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json"));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, timeWindowInSeconds);
		return conf;
	}

}
