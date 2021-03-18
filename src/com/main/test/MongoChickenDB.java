package com.main.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;

public class MongoChickenDB {
	
	//服务器地址：localhost(本地)，端口号：27017
	private final static String dbname = "chicken";
	private final static String ip = "129.204.171.195";
	private final static int port = 27017;
        //mongoClient使用单例模式创建
        private volatile static MongoClient mongoClient = null;     
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        
        
	private static MongoCollection<Document> getCollection(String collectionname){
            //实例化一个mongo客户端,使用双重检查锁定来保证线程安全
            if(mongoClient == null){
                synchronized (MongoChickenDB.class){
                    if(mongoClient == null){
                        mongoClient = new MongoClient(ip,port);
                    }
                }
            }
            //实例化一个mongo数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase(dbname);
            //获取数据库中某个集合
            MongoCollection<Document> collection = mongoDatabase.getCollection(collectionname);
            return collection;
    }
	
	//插入一条鸡的数据（包含向该鸡的集合插入一条历史数据 以及向实时表更新鸡的数据）
	public void insert(String tagId,String rss, String phase, String dopplerShift, String antenna, String frequency) {
		try {
			 MongoCollection<Document> collection1 = getCollection("latest");
			 MongoCollection<Document> collection2 = getCollection("c"+tagId);
			 Document doc1 = new Document("_id","c"+tagId).append("rss", rss).append("phase", phase).append("dopplerShift", dopplerShift).append("antenna", antenna).append("frequency", frequency);
             collection1.updateOne(Filters.eq("_id", "c"+tagId), new Document("$set", doc1).append("$currentDate", new Document("dateTime", true)), (new UpdateOptions()).upsert(true));
             Document doc2 = new Document("tagId","c"+tagId).append("rss", rss).append("phase", phase).append("dopplerShift", dopplerShift).append("antenna", antenna).append("frequency", frequency);
             collection2.updateOne(Filters.eq("tagId", "c"+0), new Document("$set", doc2).append("$currentDate", new Document("dateTime", true)), (new UpdateOptions()).upsert(true));
		
		}catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
	}
        
        //插入一堆鸡的数据（包含向该鸡的集合插入一条历史数据 以及向实时表更新鸡的数据）
	public void insert(List<String[]> list) {
		try {
                    MongoCollection<Document> collection1 = getCollection("latest");
                    for(int i=0; i<list.size(); i++){
                        MongoCollection<Document> collection2 = getCollection("c"+list.get(i)[0]);
                        Document doc1 = new Document("_id","c"+list.get(i)[0]).append("rss", list.get(i)[1]).append("phase", list.get(i)[2]).append("dopplerShift", list.get(i)[3]).append("antenna", list.get(i)[4]).append("frequency", list.get(i)[5]);
                        collection1.updateOne(Filters.eq("_id", "c"+list.get(i)[0]), new Document("$set", doc1).append("$currentDate", new Document("dateTime", true)), (new UpdateOptions()).upsert(true));
                        Document doc2 = new Document("tagId","c"+list.get(i)[0]).append("rss", list.get(i)[1]).append("phase", list.get(i)[2]).append("dopplerShift", list.get(i)[3]).append("antenna", list.get(i)[4]).append("frequency", list.get(i)[5]);
                        collection2.updateOne(Filters.eq("tagId", "c"+0), new Document("$set", doc2).append("$currentDate", new Document("dateTime", true)), (new UpdateOptions()).upsert(true));
                        //collection2.insertOne(doc2);
                    }
			
		}catch(Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
	}
	
	//查找某只鸡过去一段时间的历史数据
	public List<String[]> findHistory(String tagId, int month, int day, int hour, int minute) {
		tagId = "c"+tagId;
		
		Calendar c1 = Calendar.getInstance();
		c1.add(Calendar.MONTH, -month);
		c1.add(Calendar.DATE, -day);
		c1.add(Calendar.HOUR, -hour);
		c1.add(Calendar.MINUTE, -minute);
		
		List<String[]> res = new LinkedList<String[]>();
		try{
                    MongoCollection<Document> collection = getCollection(tagId);
                    MongoCursor<Document>  cursor= collection.find(Filters.gt("dateTime", c1.getTime())).iterator();
                    while(cursor.hasNext()){
                        Document line = cursor.next();
                        String[] str = new String[7];
                        str[0] = line.getString("tagId").substring(1);
                        str[1] = line.getString("rss");
                        str[2] = line.getString("phase");
                        str[3] = line.getString("dopplerShift");
                        str[4] = line.getString("antenna");
                        str[5] = line.getString("frequency");
                        str[6] = sdf.format(line.getDate("dateTime"));
                        //System.out.println(cursor.next().toJson());
                        /*System.out.println(str[0]);
                        System.out.println(str[1]);
                        System.out.println(str[2]);
                        System.out.println(str[3]);
                        System.out.println(str[4]);
                        System.out.println(str[5]);
                        System.out.println(str[6]);*/
                        res.add(str);
                    }
                }catch(Exception e){
                    System.err.println(e.getClass().getName() + ": " + e.getMessage());
                }
	
		return res;
	}
	
	//查找某只鸡所有历史数据
		public List<String[]> findHistory(String tagId) {
			tagId = "c"+tagId;
			List<String[]> res = new LinkedList<String[]>();
			try{
                            MongoCollection<Document> collection = getCollection(tagId);
                            MongoCursor<Document>  cursor= collection.find(Filters.eq("tagId", tagId)).iterator();
                            while(cursor.hasNext()){
                                Document line = cursor.next();
                                String[] str = new String[7];
                                str[0] = line.getString("tagId").substring(1);
                                str[1] = line.getString("rss");
                                str[2] = line.getString("phase");
                                str[3] = line.getString("dopplerShift");
                                str[4] = line.getString("antenna");
                                str[5] = line.getString("frequency");
                                str[6] = sdf.format(line.getDate("dateTime"));
                                res.add(str);
                            }
                        }catch(Exception e){
                            System.err.println(e.getClass().getName() + ": " + e.getMessage());
                        }
		
			return res;
		}
	
	//查找过去一段时间内出现的鸡,参数为当前时间减去的月数、天数、时间数、分钟数。
	//for example: findAll(0,1,0,0) 近一天内reader探测到的野鸡数据。
	public List<String[]> findAll(int month, int day, int hour, int minute) {
		Calendar c1 = Calendar.getInstance();
		c1.add(Calendar.MONTH, -month);
		c1.add(Calendar.DATE, -day);
		c1.add(Calendar.HOUR, -hour);
		c1.add(Calendar.MINUTE, -minute);
		
		List<String[]> res = new LinkedList<String[]>();
		try{
                    MongoCollection<Document> collection = getCollection("latest");
                    MongoCursor<Document>  cursor= collection.find(Filters.gt("dateTime", c1.getTime())).iterator();
                    while(cursor.hasNext()){
                        Document line = cursor.next();
                        String[] str = new String[7];
                        str[0] = line.getString("_id").substring(1);
                        str[1] = line.getString("rss");
                        str[2] = line.getString("phase");
                        str[3] = line.getString("dopplerShift");
                        str[4] = line.getString("antenna");
                        str[5] = line.getString("frequency");
                        str[6] = sdf.format(line.getDate("dateTime"));
                        res.add(str);
                    }
                }catch(Exception e){
                    System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                }
		return res;
	}
	
	//查找所有鸡的当前数据
	public List<String[]> findAllNow() {
		List<String[]> res = new LinkedList<String[]>();
		try{
                    MongoCollection<Document> collection = getCollection("latest");
                    MongoCursor<Document>  cursor= collection.find().iterator();
                    while(cursor.hasNext()){
                        Document line = cursor.next();
                        String[] str = new String[7];
                        str[0] = line.getString("_id").substring(1);
                        str[1] = line.getString("rss");
                        str[2] = line.getString("phase");
                        str[3] = line.getString("dopplerShift");
                        str[4] = line.getString("antenna");
                        str[5] = line.getString("frequency");
                        str[6] = sdf.format(line.getDate("dateTime"));
                        res.add(str);
                    }
                }catch(Exception e){
                    System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                }
		return res;
	}
	
	//删除某只鸡的数据
	public long delete(String tagId) {
		tagId = "c"+tagId;
		long collectionCount = 0;
		Document deleteResult = null;
		try{
                    MongoCollection<Document> collection1 = getCollection(tagId);
                    collectionCount = collection1.count();
                    collection1.drop();
                    MongoCollection<Document> collection2 = getCollection("latest");
                    deleteResult = collection2.findOneAndDelete(Filters.eq("_id", tagId));
                }catch(Exception e){
                    System.err.println(e.getClass().getName() + ": " + e.getMessage());
                }
		return collectionCount;
	}
	
	//删除所有鸡
	public long deleteAll() {
		long collectionCount = 0;
		try{
                        MongoClient  mongoClient = new MongoClient(ip,port);
                        MongoDatabase mongoDatabase = mongoClient.getDatabase(dbname);
                        MongoCollection<Document> collection1 = getCollection("latest");
                        collectionCount = collection1.count();
                        mongoDatabase.drop();
                }catch(Exception e){
                    System.err.println(e.getClass().getName() + ": " + e.getMessage());
                }
		return collectionCount;
	}
	
}
