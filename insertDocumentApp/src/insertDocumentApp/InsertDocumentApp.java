package insertDocumentApp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;

import javax.swing.text.Document;

import com.mongodb.Cursor;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

public class InsertDocumentApp {
	public static void main(String[] args) {

		MongoClient mongoClient = new MongoClient("localhost", 27017);
		MongoDatabase database = mongoClient.getDatabase("hw3");
		MongoCollection<org.bson.Document> movie = database.getCollection("movies");
		MongoCollection<org.bson.Document> rating = database.getCollection("ratings");
		MongoCollection<org.bson.Document> user = database.getCollection("users");
		/*
		 * URL path = ClassLoader.getSystemResource("movies.csv"); if (path ==
		 * null) { //System.out.println("No Such File"); }
		 */
		// movie file
		String movieFile = "doc/movies.dat";
		File file = new File(new File(movieFile).getAbsolutePath());
		// System.out.println(new File("movies.dat").getAbsolutePath());

		try {
			BufferedReader br = new BufferedReader(new FileReader(file));
			String line = null;
			List<org.bson.Document> movies = new ArrayList<org.bson.Document>();
			int counter = 0;
			while ((line = br.readLine()) != null) {
				String data = line;
				// data = data.replaceAll(",", ",,");
				String[] value = data.split("::");
				Integer movieId = 0;
				String title = "";
				String genres = "";
				if (value.length == 3) {
					movieId = Integer.parseInt(value[0]);
					title = value[1];
					genres = value[2];
					movies.add(
							new org.bson.Document("movieId", movieId).append("title", title).append("genres", genres));
				} else if (value.length == 2) {
					movieId = Integer.parseInt(value[0]);
					title = value[1];
					movies.add(new org.bson.Document("movieId", movieId).append("title", title));
				} else {
					movieId = Integer.parseInt(value[0]);
					movies.add(new org.bson.Document("movieId", movieId));
				}
				// sparse --> don't have to store too many columns
				counter++;
				if (counter > 500) {
					movie.insertMany(movies);
					movies.clear();
					counter = 0;
				}
				// System.out.println(movieId +"**"+ title +"**"+genres);

			}
			movie.insertMany(movies);

			org.bson.Document myDoc = movie.find().first();
			System.out.println((myDoc).toJson());
			/*
			 * MongoCursor<org.bson.Document> cursor = movie.find().iterator();
			 * try { while (cursor.hasNext()) {
			 * System.out.println(cursor.next().toJson()); } } finally {
			 * cursor.close(); }
			 */
			// drop collection
			// movie.drop();
			// System.out.println("Clear all? Count:" + movie.count());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Ratings file
		String ratingFile = "doc/ratings.dat";
		File file2 = new File(new File(ratingFile).getAbsolutePath());
		System.out.println(new File("ratings.dat").getAbsolutePath());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file2));
			String line = null;
			List<org.bson.Document> ratings = new ArrayList<org.bson.Document>();
			int counter = 0;
			DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
			while ((line = br.readLine()) != null) {
				String data = line;
				// data = data.replaceAll(",", ",,");
				String[] value = data.split("::");
				Integer userId = 0;
				Integer movieId = 0;
				Double r = 0.0;
				long time = 0L;
				String timestamp = null;

				if (value.length == 4) {
					userId = Integer.parseInt(value[0]);
					movieId = Integer.parseInt(value[1]);
					r = Double.parseDouble(value[2]);
					time = Long.parseLong(value[3]);
					Date date = new Date(time);
					timestamp = formatter.format(date);

					// //System.out.println("Converted UTC TIME (using Format
					// method) : "+dateString);
					/*
					 * try { timestamp = formatter.parse(dateString); } catch
					 * (ParseException e) { e.printStackTrace(); }
					 */
					ratings.add(new org.bson.Document("userId", userId).append("movieId", movieId).append("ratings", r)
							.append("timestamp", timestamp));
				} else if (value.length == 3) {
					userId = Integer.parseInt(value[0]);
					movieId = Integer.parseInt(value[1]);
					r = Double.parseDouble(value[2]);
					ratings.add(
							new org.bson.Document("userId", userId).append("movieId", movieId).append("ratings", r));
				} else if (value.length == 2) {
					userId = Integer.parseInt(value[0]);
					movieId = Integer.parseInt(value[1]);
					ratings.add(new org.bson.Document("userId", userId).append("movieId", movieId));
				} else {
					userId = Integer.parseInt(value[0]);
					ratings.add(new org.bson.Document("userId", userId));
				}
				// sparse --> don't have to store too many columns
				counter++;
				if (counter > 500) {
					rating.insertMany(ratings);
					ratings.clear();
					// System.out.println(counter + " ratings inserted...");
					counter = 0;
				}
				// System.out.println(userId + "**" + movieId +"**"+ ratings
				// +"**"+timestamp);

			}
			rating.insertMany(ratings);
			/*
			 * org.bson.Document myDoc = rating.find().first();
			 * //System.out.println((myDoc).toJson());
			 */
			org.bson.Document myDoc = rating.find().first();
			System.out.println((myDoc).toJson());

			/*
			 * MongoCursor<org.bson.Document> cursor = rating.find().iterator();
			 * try { while (cursor.hasNext()) { //
			 * System.out.println(cursor.next().toJson()); } } finally {
			 * cursor.close(); }
			 */
			// drop collection
			// rating.drop();
			// System.out.println("Clear all? Count:" + rating.count());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// tags file
		String usersFile = "doc/users.dat";
		File file3 = new File(new File(usersFile).getAbsolutePath());
		// System.out.println(new File("tags.dat").getAbsolutePath());
		try {
			BufferedReader br = new BufferedReader(new FileReader(file3));
			String line = null;
			List<org.bson.Document> users = new ArrayList<org.bson.Document>();
			//DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			//formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
			int counter = 0;
			while ((line = br.readLine()) != null) {
				String data = line;
				// data = data.replaceAll(",", ",,");
				String[] value = data.split("::");
				Integer userId = 0;
				String gender = "";
				String age = "";
				//long time = 0L;
				String occupation = "";
				String zipCode= "";

				if (value.length == 5) {
					userId = Integer.parseInt(value[0]);
					gender = value[1];
					age = value[2];
					occupation = value[3];
					zipCode = value[4];
					//Date date = new Date(time);
					//timestamp = formatter.format(date);

					// //System.out.println("Converted UTC TIME (using Format
					// method) : "+dateString);

					users.add(new org.bson.Document("userId", userId).append("gender", gender).append("age", age)
							.append("occupation", occupation).append("zipCode", zipCode));
				} else if (value.length == 4) {
					userId = Integer.parseInt(value[0]);
					gender = value[1];
					age = value[2];
					occupation = value[3];
					users.add(new org.bson.Document("userId", userId).append("gender", gender).append("age", age).append("occupation", occupation));
				} else if (value.length == 3) {
					userId = Integer.parseInt(value[0]);
					gender = value[1];
					age = value[2];
					users.add(new org.bson.Document("userId", userId).append("gender", gender).append("age", age));
				} else if (value.length == 2) {
					userId = Integer.parseInt(value[0]);
					gender = value[1];
					users.add(new org.bson.Document("userId", userId).append("gender", gender));
				}else {
					userId = Integer.parseInt(value[0]);
				}
				// sparse --> don't have to store too many columns
				counter++;
				if (counter > 500) {
					user.insertMany(users);
					users.clear();
					counter = 0;
				}
				// System.out.println(userId + "**" + movieId +"**"+ tags
				// +"**"+timestamp);

			}

			user.insertMany(users);
			org.bson.Document myDoc = user.find().first();
			System.out.println((myDoc).toJson());
			/*
			 * MongoCursor<org.bson.Document> cursor = rating.find().iterator();
			 * try{ while (cursor.hasNext()) {
			 * //System.out.println(cursor.next().toJson()); } }finally{
			 * cursor.close(); }
			 */
			// drop collection
			// tag.drop();
			// System.out.println("Clear all? Count:" + tag.count());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		/*
		 * old file IO File file = new File(new
		 * File(movieFile).getAbsolutePath()); //System.out.println(new
		 * File("movies.dat").getAbsolutePath()); try { Scanner inputStream =
		 * new Scanner(file); // skip the headline //inputStream.next();
		 * List<org.bson.Document> movies = new ArrayList<org.bson.Document>();
		 * while (inputStream.hasNext()) { String data = inputStream.nextLine();
		 * //data = data.replaceAll(",", ",,"); String[] value
		 * =data.split("::"); String movieId = ""; String title = ""; String
		 * genres = ""; if (value.length == 3) { movieId = value[0]; title =
		 * value[1]; genres = value[2]; }else if (value.length == 2) { movieId =
		 * value[0]; title = value[1]; }else { movieId = value[0]; }
		 * 
		 * movies.add(new org.bson.Document("movieId",movieId).append("title",
		 * title).append("genres", genres)); //System.out.println(movieId +"**"+
		 * title +"**"+genres);
		 * 
		 * } movie.insertMany(movies); // DeleteResult deleteResult =
		 * movie.deleteMany(gte("movieId",23));
		 * 
		 * MongoCursor<org.bson.Document> cursor = movie.find().iterator(); try{
		 * while (cursor.hasNext()) {
		 * //System.out.println(cursor.next().toJson()); } }finally{
		 * cursor.close(); } //drop collection //movie.drop();
		 * //System.out.println("Clear all? Count:" + movie.count());
		 * inputStream.close(); } catch (FileNotFoundException e) { // TODO
		 * Auto-generated catch block e.printStackTrace();
		 * //System.out.println(e); }
		 */
	}

}
