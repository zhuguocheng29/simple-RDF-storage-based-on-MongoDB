package com.db.seu.MongodbTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	   try {  
               //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址  
               //ServerAddress()两个参数分别为 服务器地址 和 端口  
               ServerAddress serverAddress = new ServerAddress("223.3.84.42",27017);  
               List<ServerAddress> addrs = new ArrayList<ServerAddress>();  
               addrs.add(serverAddress);  
                 
               //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码  
/*               MongoCredential credential = MongoCredential.createScramSha1Credential(null, "task", null);  
               List<MongoCredential> credentials = new ArrayList<MongoCredential>();  
               credentials.add(credential);  */
                 
               //通过连接认证获取MongoDB连接  
               MongoClient mongoClient = new MongoClient(addrs);  
                 
               //连接到数据库  
               MongoDatabase mongoDatabase = mongoClient.getDatabase("task");  
               System.out.println("Connect to database successfully");  
               
               //插入新表
               //addTable(mongoDatabase);
               
               //检索所有文档  
               /** 
                * 1. 获取迭代器FindIterable<Document> 
                * 2. 获取游标MongoCursor<Document> 
                * 3. 通过游标遍历检索出的文档集合 
                * */  
              
        	   System.out.println("请按照如下格式输入： ");
        	   System.out.println("数字");
        	   System.out.println("查询字段");
        	   System.out.println();
        	   System.out.println("其中:");
        	   System.out.println("数字1表示查询三元组信息");
        	   System.out.println("数字2表示查所属类信息");
        	   System.out.println("数字3表示查父子类信息");
               
               while(1 != 0){

            	   Scanner sc = new Scanner(System.in);
            	   int i = sc.nextInt();
            	   sc.nextLine();
            	   String input = sc.nextLine();
            	   if(i == 1){
            		   System.out.println("查询有关\""+input+"\"的三元组信息");
            		   //查询1：搜军长，查statement中的三元组
		                  MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("RDF_RESOURCE");
		                  FindIterable<Document> findIterable = mongoCollection.find(Filters.eq("resource", input));  
		                  MongoCursor<Document> mongoCursor = findIterable.iterator();  
		                  
		                  MongoCollection<Document> mongoC = mongoDatabase.getCollection("RDF_STATEMENT");
	
		                  while(mongoCursor.hasNext()){
		               	   Document d = mongoCursor.next();
		                      FindIterable<Document> findIterable1 = mongoC.find(Filters.in("subjectID", d.get("_id")));
		                      Document doc = findIterable1.first();
		                      FindIterable<Document> subject = mongoCollection.find(Filters.in("_id", doc.get("subjectID")));
		                      FindIterable<Document> predict = mongoCollection.find(Filters.in("_id", doc.get("predictID")));
		                      FindIterable<Document> object = mongoCollection.find(Filters.in("_id", doc.get("objectID")));
		                      System.out.println(subject.first().getString("resource") + " " + predict.first().getString("resource")+ " " + object.first().getString("resource"));
		                      System.out.println();
		                  }
            	   }else if(i == 2){
            		   System.out.println("查询有关\""+input+"\"的所属类");
            		   //查询2：搜军长，查军长属于哪个类
                       MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("RDF_RESOURCE");
                       FindIterable<Document> findIterable = mongoCollection.find(Filters.eq("resource", input));  
                       MongoCursor<Document> mongoCursor = findIterable.iterator();  
                       MongoCollection<Document> mongoC = mongoDatabase.getCollection("RDF_CLASS");
                       while(mongoCursor.hasNext()){
                    	   Document d = mongoCursor.next();
                           FindIterable<Document> findIterable1 = mongoC.find(Filters.in("_id", d.get("classType")));
                           Document doc = findIterable1.first();
                           FindIterable<Document> subject = mongoCollection.find(Filters.in("_id", doc.get("rid")));
                           System.out.println(input +" "+"属于"+" "+subject.first().getString("resource"));
                           System.out.println();
                       }
            	   }else{
            		   System.out.println("查询有关\""+input+"\"的所属父类");
            		   //查询3：搜军长，查军长属于哪个父类
                       MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("RDF_RESOURCE");
                       FindIterable<Document> findIterable = mongoCollection.find(Filters.eq("resource", input));  
                       MongoCursor<Document> mongoCursor = findIterable.iterator();  
                       MongoCollection<Document> mongoC = mongoDatabase.getCollection("RDF_CLASS");
                       MongoCollection<Document> mongoS = mongoDatabase.getCollection("RDF_SUBCLASSOF");
                       while(mongoCursor.hasNext()){
                    	   Document d = mongoCursor.next();
                           FindIterable<Document> findIterable1 = mongoC.find(Filters.in("_id", d.get("classType")));
                           Document doc = findIterable1.first();
                           FindIterable<Document> subject = mongoCollection.find(Filters.in("_id", doc.get("rid")));
                           Document child = subject.first();
                           FindIterable<Document> childD = mongoS.find(Filters.in("subID", child.get("classID")));
                           Document parent = childD.first();
                           FindIterable<Document> parentD = mongoCollection.find(Filters.in("classID", parent.get("superID")));
                           System.out.println(input +" "+"属于"+" "+subject.first().getString("resource"));
                           System.out.println(subject.first().getString("resource")+" "+"subClassOf"+" "+parentD.first().getString("resource"));
                           System.out.println();
                       }
            	   }
               }
              
           } catch (Exception e) {  
               System.err.println( e.getClass().getName() + ": " + e.getMessage() );  
           }  
    }
    public static void addTable(MongoDatabase mongoDatabase){
        //创建集合 参数为 “集合名称”  
        mongoDatabase.createCollection("RDF_RESOURCE");
        mongoDatabase.createCollection("RDF_STATEMENT");
        mongoDatabase.createCollection("RDF_SUBCLASSOF");
        mongoDatabase.createCollection("RDF_CLASS");
        System.out.println("Collection created successfully");
        
        //获取集合 参数为“集合名称”  
/*               MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("RDF_RESOURCE");  
        
        System.out.println("Collection mycol selected successfully");
        
        String id = "1";
        String id1 = "2";
        String id2 = "3";
        String id3 = "4";
        String id4 = "5";

        Document document = new Document("_id", id).  
                append("resource", "军长").append("classType", id1 ); 
        Document document1 = new Document("_id", id1).append("propertyType", id).  
                append("resource", "发出"); 
        Document document2 = new Document("_id", id2).append("classType",id).  
                append("resource", "命令"); 
        Document document3 = new Document("_id", id3).append("classID",id2). 
                append("resource", "国防部"); 
        Document document4 = new Document("_id", id4).append("classID",id1). 
                append("resource", "部队"); 
        
        List<Document> documents = new ArrayList<Document>();  
        documents.add(document);
        documents.add(document1); 
        documents.add(document2);
        documents.add(document3);
        documents.add(document4);
        mongoCollection.insertMany(documents); 
        System.out.println("RESOURCE inserted successfully");*/
        
        
    	MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("RDF_STATEMENT");  
        
        System.out.println("Collection mycol selected successfully");
        
        String id = "1";
        String id1 = "2";
        String id2 = "3";
        String id3 = "4";
        String id4 = "5";

        Document document = new Document("_id", id)
     		   .append("subjectID", id)
     		   .append("predictID", id1)
     		   .append("objectID", id2); 

        
        List<Document> documents = new ArrayList<Document>();  
        documents.add(document);
        mongoCollection.insertMany(documents); 
        System.out.println("class inserted successfully");
    }
}
