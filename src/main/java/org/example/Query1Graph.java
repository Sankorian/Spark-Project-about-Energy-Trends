package org.example;

import org.knowm.xchart.*;
import java.io.*;
import java.util.*;

public class Query1Graph {
    public static void main(String[] args) throws Exception {
        // Reading CSV File with a BufferedReader br
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\daost\\Desktop\\Sistemi e Architetture per Big Data\\Progetto1 Information and Data\\query1_results.csv"));
        
        //Creating important arrays for the chart
        List<Integer> years = new ArrayList<>();
        List<Double> italyCarbon = new ArrayList<>();
        List<Double> swedenCarbon = new ArrayList<>();
        List<Double> italyCFE = new ArrayList<>();
        List<Double> swedenCFE = new ArrayList<>();

        // Filling arrays
        String line;
        while ((line = br.readLine()) != null) {
            String[] data = line.split(",");
            if (!data[0].equals("Year")) { // Skips the header
                int year = Integer.parseInt(data[0]);
                if (!years.contains(year)) { // Adds year once
                    years.add(year);
                }

                if (data[1].equals("Italy")) {
                    italyCarbon.add(Double.parseDouble(data[2])); // Avg Carbon Intensity
                    italyCFE.add(Double.parseDouble(data[5]));    // Avg CFE %
                }
                if (data[1].equals("Sweden")) {
                    swedenCarbon.add(Double.parseDouble(data[2])); // Avg Carbon Intensity
                    swedenCFE.add(Double.parseDouble(data[5]));    // Avg CFE %
                }
            }
        }
        br.close();

        // Creating Graph 1: Carbon Intensity Trends
        XYChart carbonChart = new XYChartBuilder().width(800).height(600)
                .title("Carbon Intensity Trends")
                .xAxisTitle("Year")
                .yAxisTitle("Avg Carbon Intensity (gCO₂/kWh)")
                .build();
        carbonChart.addSeries("Italy", years, italyCarbon);
        carbonChart.addSeries("Sweden", years, swedenCarbon);
        carbonChart.getStyler().setXAxisDecimalPattern("0"); // Ensures whole year numbers

        new SwingWrapper<>(carbonChart).displayChart();

        // Creating Graph 2: Carbon-Free Energy (CFE%) Trends
        XYChart cfeChart = new XYChartBuilder().width(800).height(600)
                .title("Carbon-Free Energy Trends")
                .xAxisTitle("Year")
                .yAxisTitle("Avg CFE (%)")
                .build();
        cfeChart.addSeries("Italy", years, italyCFE);
        cfeChart.addSeries("Sweden", years, swedenCFE);
        cfeChart.getStyler().setYAxisMin(0.0);  // Minimum value is set to 0
        cfeChart.getStyler().setXAxisDecimalPattern("0"); // Ensures whole year numbers
        new SwingWrapper<>(cfeChart).displayChart();
    }
}

