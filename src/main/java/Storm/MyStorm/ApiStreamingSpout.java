package Storm.MyStorm;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.esotericsoftware.minlog.Log;

import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ApiStreamingSpout extends BaseRichSpout{
	static String STREAMING_API_URL="https://stream.twitter.com/1/statues/filter.json?track=";
	private String track;
	private String user;
	private String password;
	private UsernamePasswordCredentials credentials;
	private BasicCredentialsProvider credentialProvider;
	private DefaultHttpClient client;
	private SpoutOutputCollector collector;
	static JSONParser jsonParser = new JSONParser();
	
	public void nextTuple() {
		client = new DefaultHttpClient();
		client.setCredentialsProvider(credentialProvider);
		HttpGet get = new HttpGet(STREAMING_API_URL + track);
		HttpResponse response;
		try {
			//excute
			response = client.execute(get);
			StatusLine status = (StatusLine)response.getStatusLine();
			if(status.getStatusCode() == 200) {
				InputStream inputStream = response.getEntity().getContent();
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				String in;
				//Read line by line
				while((in = reader.readLine()) != null) {
					try {
						//Prase and emit
						Object json = jsonParser.parse(in);
						collector.emit(new Values(track, json));
					} catch(ParseException e) {
						Log.error("Error parsing message from twitter", e);
					}
				} 
			}
		} catch (IOException e) {
			Log.error("Error in communication with twitter api [" + get.getURI().toString() + "]");
			try {
				Thread.sleep(10000);
			} catch(InterruptedException e1) {
			}
		}
	}
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		int spoutsSize = context.getComponentTasks(context.getThisComponentId()).size();
		int myIdx = context.getThisTaskIndex();
		String[] tracks = ((String) conf.get("track")).split(",");
		StringBuffer tracksBuffer = new StringBuffer();
		for(int i = 0; i < tracks.length; i ++) {
			if(i % spoutsSize == myIdx) {
				tracksBuffer.append(",");
				tracksBuffer.append(tracks[i]);
			}
		}
		
		if(tracksBuffer.length() == 0) 
			throw new RuntimeException("No track found for support" + 
				"[spoutsSzie:" + spoutsSize + ", tracks:" + tracks.length + "] the amount" +
				" of tracks must be more then the spout paralellism");
		
		this.track = tracksBuffer.substring(1).toString();
		
		user = (String)conf.get("user");
		password = (String)conf.get("password");
		
		credentials = new UsernamePasswordCredentials(user, password);
		credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, credentials);
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("criteria", "tweet"));
	}
}
