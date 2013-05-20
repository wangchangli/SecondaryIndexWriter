package com.xingcloud.xa.secondaryindex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    try{
      Map<String, Map<String, List<Index>>> putsMap =  dispatchPuts(logs);
      ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(Constants.EXECUTOR_THREAD_COUNT,
        Constants.EXECUTOR_THREAD_COUNT,
        30,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<Runnable>());
      
      for(Map.Entry<String, Map<String, List<Index>>> entry: putsMap.entrySet()){
        threadPoolExecutor.execute(new HPutTask(entry.getKey(), entry.getValue()));
      }
      
      threadPoolExecutor.shutdown();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void run() {
    try{
      this.start();
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }
  
  private Map<String, Map<String, List<Index>>> dispatchPuts(List<String> logs) throws IOException {
    Map<String, Map<String, List<Index>>> putsMap = new HashMap<String, Map<String, List<Index>>>();
    ObjectMapper mapper = new ObjectMapper();
    for(String log: logs){
      
      Map<String,Object> data = mapper.readValue(log.getBytes(), Map.class);
      String projectID = (String)data.get("pid");
      String uid = (String)data.get("uid");
      String propertyID = (String)data.get("propertyID");
      String oldValue = (String)data.get("oldValue");
      String newValue = (String)data.get("newValue");
      Boolean needDelete = (Boolean)data.get("delete");
      
      Index put = new Index(projectID, uid, propertyID, newValue, "put");   
      Index delete = null;
      
      if (needDelete){
        delete = new Index(projectID, uid, propertyID, oldValue, "delete");    
      }

      //String hbaseAddress = UidMappingUtil.getInstance().hash(Long.valueOf(uid));
      String hbaseAddress = "localhost";
      
      if(! putsMap.containsKey(projectID)){
        putsMap.put(projectID, new HashMap<String, List<Index>>());
      }
      
      if(! putsMap.get(projectID).containsKey(hbaseAddress)){
        putsMap.get(projectID).put(hbaseAddress, new ArrayList<Index>());   
      }
      
      if (delete != null ) putsMap.get(projectID).get(hbaseAddress).add(delete);
      putsMap.get(projectID).get(hbaseAddress).add(put);
      
    }  
    return putsMap;
  }
  
  
}
