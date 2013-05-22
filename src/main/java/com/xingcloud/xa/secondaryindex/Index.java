package com.xingcloud.xa.secondaryindex;

/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 5/20/13
 * Time: 7:55 AM
 * To change this template use File | Settings | File Templates.
 */
public class Index {
  private String projectID;
  private long uid;
  private int propertyID;
  private String value="";
  private String operation;
  
  public Index(String projectID, long uid, int propertyID , String value, String operation){
    this.projectID = projectID;
    this.uid = uid;
    this.propertyID = propertyID;
    this.value = value;
    this.operation = operation;
  }


  public String getProjectID() {
    return projectID;
  }

  public long getUid() {
    return uid;
  }

  public int getPropertyID() {
    return propertyID;
  }

  public String getValue() {
    return value;
  }

  public String getOperation() {
    return operation;
  }
  
  @Override
  public boolean equals(Object o){
    return (this.hashCode() == o.hashCode());  
  }
  @Override
  public int hashCode(){
    return (projectID + "_" + propertyID + "_" + value +"_"+ uid).hashCode();  
  }


  @Override
  public String toString(){
    return operation+"\t"+projectID+"\t"+uid+"\t"+propertyID+"\t"+value;
  }
  
  public void setProjectID(String projectID) {
    this.projectID = projectID;
  }

  public void setUid(long uid) {
    this.uid = uid;
  }

  public void setPropertyID(int propertyID) {
    this.propertyID = propertyID;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }
}
