package com.xingcloud.xa.secondaryindex;

import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 5/17/13
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class HPutTask implements Runnable  {
  
  private String projectID;
  private List<Put> puts;
  
  public HPutTask(String projectID, List<Put> puts){
    
    this.projectID = projectID;
    this.puts = puts;
  }
  
  @Override
  public void run() {
      
  }
}
