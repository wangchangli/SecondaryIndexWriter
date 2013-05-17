package com.xingcloud.xa.secondaryindex;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/17/13
 * Time: 9:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexTailer extends Tail implements Runnable{

  private static final Log LOG = LogFactory.getLog(SecondaryIndexWriter.class);
  
  public IndexTailer(String configPath) {
    super(configPath);
    setBatchSize(10000);
    setLogProcessPerBatch(true);
  }

  @Override
  public void send(List<String> logs, long day) {
    Map<String, List<Put>> putsMap =  dispatchPuts(logs);
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(Constants.EXECUTOR_THREAD_COUNT,
      Constants.EXECUTOR_THREAD_COUNT,
      30,
      TimeUnit.MINUTES,
      new LinkedBlockingQueue<Runnable>());
    
    for(Map.Entry<String, List<Put>> entry: putsMap.entrySet()){
      threadPoolExecutor.execute(new HPutTask(entry.getKey(), entry.getValue()));
    }
    
    threadPoolExecutor.shutdown();
    
  }

  @Override
  public void run() {
    try{
      this.start();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }
  
  private Map<String, List<Put>> dispatchPuts(List<String> logs) throws IOException {
    Map<String, List<Put>> putsMap = new HashMap<String, List<Put>>();
    ObjectMapper mapper = new ObjectMapper();
    for(String log: logs){
      Map<String,Object> data = mapper.readValue(log.getBytes(), Map.class);
      String projectID = (String)data.get("pid");
      String uid = (String)data.get("uid");
      String propertyID = (String)data.get("propertyID");
      String oldValue = (String)data.get("oldValue");
      String newValue = (String)data.get("newValue");
      Boolean delete = (Boolean)data.get("delete");

      Put put = new Put();
      put.setWriteToWAL(Constants.deuTableWalSwitch);
      
      if(putsMap.containsKey(projectID)){        
        putsMap.get(projectID).add(put);
      }else{
        putsMap.put(projectID, new ArrayList<Put>());
        putsMap.get(projectID).add(put);
      }
      
    }  
    
    
    return putsMap;
  }
  
  
}
