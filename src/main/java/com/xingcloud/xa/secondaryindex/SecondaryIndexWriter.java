package com.xingcloud.xa.secondaryindex;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 5/16/13
 * Time: 9:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class SecondaryIndexWriter {
  private static final Log LOG = LogFactory.getLog(SecondaryIndexWriter.class);
  
  public static void main(String args[]){
      new Thread(new IndexTailer("/data/log/secondaryindexconfig/")).start();
    
//    Index index1 = new Index("sof-dsk","1","grade","1","put");
//    Index index2 = new Index("sof-dsk","1","grade","2","delete");
//    Index index3 = new Index("sof-dsk","2","grade","1","delete");
//    Map<Index, Integer> map = new HashMap<Index, Integer>();
//    map.put(index1,1);
//    map.put(index2,2);
//    map.put(index3,3);
//    System.out.println(map.size());
//    System.out.println(index1.toString()+index1.hashCode());
//    System.out.println(index2.toString()+index2.hashCode());
//    System.out.println(index3.toString()+index3.hashCode());
//    System.out.println(map.get(index2));
//    System.out.println(map.containsKey(index2));
//    System.out.println(map.containsKey(index3)); 
  }
}
