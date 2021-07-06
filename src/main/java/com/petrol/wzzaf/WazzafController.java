package com.petrol.wzzaf;

import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping(path= "api")
public class WazzafController {
    private final WazzafService dataQuery ;
    @Autowired
    public WazzafController(WazzafService dataQuery) {
        this.dataQuery = dataQuery;
    }


    @GetMapping(path="displaytop")
    public String read_data(){

        return dataQuery.read_data();}

    @GetMapping(path="struct")
    public String structure(){
        return dataQuery.structure();
}



    @GetMapping(path="sum")
    public String summary(){
        return dataQuery.summary();
    }


    @GetMapping(path="topcompcount")
    public String topcompanies(){
        return dataQuery.topcompanies();
    }



    @GetMapping(path="piechart")
    public String piechart() throws IOException {
        return dataQuery.piechart();
    }


    @GetMapping(path="popularjobs")
    public String popularjobs() {
        return dataQuery.popularjobs();
    }


    @GetMapping(path="barchart")
    public String barchart() throws IOException {
        return dataQuery.barchart();
    }


    @GetMapping(path="Areas")
    public String MostPopularAreas()  {
        return dataQuery.MostPopularAreas();
    }


    @GetMapping(path="areasbar")
    public String AreasBarchart() throws IOException {
        return dataQuery.AreasBarchart();
    }
    @GetMapping(path="skills")
    public String skills()   {
        return dataQuery.skills();
    }



}
