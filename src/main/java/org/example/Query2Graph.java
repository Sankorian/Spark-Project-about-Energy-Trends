package org.example;

import org.knowm.xchart.*;
import java.io.*;
import java.util.*;
import java.text.*;

public class Query2Graph {
    public static void main(String[] args) throws Exception {
        // Reading CSV File with a BufferedReader br
        // ***THIS LINE HAS TO BE EDITED ON DIFFERENT SYSTEMS!***
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\daost\\Desktop\\Sistemi e Architetture per Big Data\\Progetto1 Information and Data\\query2_results.csv"));

        // Creating important arrays for the chart
        List<Date> timeLabels = new ArrayList<>();
        List<Double> italyCarbon = new ArrayList<>();
        List<Double> italyCFE = new ArrayList<>();
        // and a Date formatter for YYYY-MM
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM"); 

        // Filling arrays
        String line;
        while ((line = br.readLine()) != null) {
            String[] data = line.split(",");
            if (!data[0].equals("year")) { // Skips the header

                // Formating year-month (YYYY-MM)
                String formattedDate = data[0] + "-" + String.format("%02d", Integer.parseInt(data[1]));
                Date time = dateFormat.parse(formattedDate);
                timeLabels.add(time);

                // Parsing String entries to numeric values
                italyCarbon.add(Double.parseDouble(data[2])); // Avg Carbon Intensity
                italyCFE.add(Double.parseDouble(data[3]));    // Avg CFE %
            }
        }
        br.close();

        // Adjusting the timeLabels-Array:
        // Adding 1 to every year because only the year of decembers are displayed
        // which leads to confusing year annotation
        for (int i = 0; i < timeLabels.size(); i++) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(timeLabels.get(i));
            cal.add(Calendar.YEAR, 1); // Add 1 year
            timeLabels.set(i, cal.getTime()); // Update the list
        }

        // Creating Graph 1: Carbon Intensity Trends (Months on X-axis)
        XYChart carbonChart = new XYChartBuilder().width(800).height(600)
                .title("Monthly Carbon Intensity Trends")
                .xAxisTitle("Year")
                .yAxisTitle("Avg Carbon Intensity (gCO₂/kWh)")
                .build();
        carbonChart.addSeries("Italy", timeLabels, italyCarbon);
        carbonChart.getStyler().setXAxisLabelRotation(45); // Rotate labels
        carbonChart.getStyler().setXAxisTickMarkSpacingHint(12);
        carbonChart.getStyler().setDatePattern("yyyy");





        new SwingWrapper<>(carbonChart).displayChart();

        // Creating Graph 2: Carbon-Free Energy (CFE%) Trends (Months on X-axis)
        XYChart cfeChart = new XYChartBuilder().width(800).height(600)
                .title("Monthly Carbon-Free Energy Trends")
                .xAxisTitle("Year")
                .yAxisTitle("Avg CFE (%)")
                .build();
        cfeChart.addSeries("Italy", timeLabels, italyCFE);
        cfeChart.getStyler().setYAxisMin(0.0);
        cfeChart.getStyler().setYAxisMax(100.0);
        cfeChart.getStyler().setXAxisLabelRotation(45); // Rotate labels


        new SwingWrapper<>(cfeChart).displayChart();
    }
}
