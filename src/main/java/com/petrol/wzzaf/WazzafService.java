package com.petrol.wzzaf;





import org.apache.commons.codec.binary.Base64;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.*;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.apache.spark.sql.*;
import org.spark_project.dmg.pmml.True;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class WazzafService implements Serializable {
	HTMLTableBuilder ht ;

    static Dataset<Row> data_set = WazzafDAO.read_csv("src/main/resources/Wuzzuf_Jobs.csv");
  	static Dataset<Row> clean_data = data_set.distinct().na().drop();
    public String read_data() {
		List<Row> top_rows = data_set.limit(8).collectAsList();
		return DisplayHtml.displayrows(data_set.columns(), top_rows);

	}


public String structure(){

	  StructType d = data_set.schema();
	   return d.prettyJson();

		}



public String summary(){

    	Dataset<Row> d = data_set.summary();
    	List<Row>samls = d.collectAsList();
    	return DisplayHtml.displayrows(d.columns(),samls) ;
}



public String topcompanies(){
    	Dataset<Row> c_No_job = data_set.select("Company").groupBy("Company")
				.count().as("job counts").orderBy(functions.col("count").desc()).limit(20);

	List<Row>samls = c_No_job.collectAsList();
	return DisplayHtml.displayrows(c_No_job.columns(),samls) ;
}



public String piechart() throws IOException {

	Dataset<Row> c_No_job = clean_data.select("Company").groupBy("Company")
			.count().as("job counts").orderBy(functions.col("count").desc()).limit(20);
	List<Row>l = c_No_job.collectAsList();


	PieChart pieChart = new PieChartBuilder().width(1024).height(768).
				title(" Number of Jobs for Each Company ").build();
        for (Row g : l) {
            String[] t = g.toString().replace("[", "")
					.replace("]", "").split(",");
            pieChart.addSeries(t[0], Integer.parseInt(t[1]));
        }

        String path = "src/main/resources/pie.png";
        BitmapEncoder.saveBitmap(pieChart,path, BitmapEncoder.BitmapFormat.PNG);
        return DisplayHtml.viewchart(path);
}



public String popularjobs(){
    	  Dataset<Row> l_job = clean_data.select("Title").groupBy("Title").
				  count().alias("job counts").orderBy(functions.col("count").desc()).limit(20);

          List<Row> l_j = l_job.collectAsList();
	return DisplayHtml.displayrows(l_job.columns(),l_j) ;

}


public String barchart() throws IOException {

	Dataset<Row> l_job = clean_data.select("Title").groupBy("Title").
			count().alias("job counts").orderBy(functions.col("count").desc()).limit(20);
	List<Row> l_j = l_job.collectAsList();

	        CategoryChart bar_chart1 = new CategoryChartBuilder().width(800).height(600).title(" Most Popular Job Titles ").xAxisTitle("Job Titles").yAxisTitle("Job counts").build();
        for (Row g : l_j) {
            String[] ll = g.toString().replace("[", "").replace("]", "").split(",");
            bar_chart1.addSeries(ll[0], new ArrayList<String>(Arrays.asList(new String[]{" "})), new ArrayList<Number>(Arrays.asList(new Number[]{Integer.parseInt(ll[1])})));
        }
		String path = "src/main/resources/bar.png";
        BitmapEncoder.saveBitmap(bar_chart1, path, BitmapEncoder.BitmapFormat.PNG);
        return DisplayHtml.viewchart(path);
}


	public String MostPopularAreas(){
    	Dataset<Row> l_Country = clean_data.select("Country").groupBy("Country").count().alias("Most Popular Areas").orderBy(functions.col("count").desc()).limit(20);
		List<Row> l_j = l_Country.collectAsList();
		return DisplayHtml.displayrows(l_Country.columns(),l_j) ;
	}



	public String AreasBarchart() throws IOException {

		Dataset<Row> l_Country = clean_data.select("Country").groupBy("Country").count().alias("Most Popular Areas").orderBy(functions.col("count").desc()).limit(20);


		List<Row> l_j = l_Country.collectAsList();

		CategoryChart bar_chart1 = new CategoryChartBuilder().width(800).height(600).title(" Most Popular Areas ").xAxisTitle("Areas").yAxisTitle("Area counts").build();
		for (Row g : l_j) {
			String[] ll = g.toString().replace("[", "").replace("]", "").split(",");
			bar_chart1.addSeries(ll[0], new ArrayList<String>(Arrays.asList(new String[]{" "})), new ArrayList<Number>(Arrays.asList(new Number[]{Integer.parseInt(ll[1])})));
		}
		String path = "src/main/resources/Areasbar.png";
		BitmapEncoder.saveBitmap(bar_chart1, path, BitmapEncoder.BitmapFormat.PNG);
		return DisplayHtml.viewchart(path);
	}



	public String skills(){
    	// convert dataset <row> into javardd
		JavaRDD<String> rdd =data_set.select("Skills").toJavaRDD().map(new Function<Row, String>() {

			@Override
			public String call(Row row) throws Exception {
				return row.toString();
			}});

		JavaRDD<String> words = rdd.flatMap (title -> Arrays.asList (title
				.toLowerCase ()
				.trim ().replace("]","").replace("[","")
				.split (", ")).iterator ());

		Map<String, Long> wordCounts = words.countByValue ();
		List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
				.sorted (Map.Entry.comparingByValue (Comparator.reverseOrder())).collect (Collectors.toList ());

		String [] header = {"skill", "count"};
	    return DisplayHtml.displymap(header,sorted);
		}

	}





























