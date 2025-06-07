Big Data Analysis of Carbon Intensity in Electricity Production using Apache Spark

This project analyzes carbon intensity and carbon-free energy share in Italy and Sweden using big data processing techniques. By leveraging Apache Spark, real-time electricity production data provided by Electricity Maps is being processed. The goal is to evaluate the sustainability of the energy grid by aggregating data over different time frames. The study focuses on monthly carbon intensity averages for Italy, yearly comparisons between Italy and Sweden, and hourly trends using percentile analysis. The experimental results reveal key insights into clean energy adoption patterns and carbon footprint trends in Europe.

The project consists of 4 java files:

Query1.java: 
Calculating yearly averages, minimum and maximum values of both carbon intensity and CFE in Italy and Sweden for every year from 2021 to 2024.

Query1Graph.java: 
Printing charts via XChart showing the data of query 1 in two graphs.

Query2.java: 
A monthly carbon intensity analysis for Italy only, determining the five highest/lowest carbon intensity and CFE months between 2021 and 2024.

Query2Graph.java: 
Printing charts via XChart showing the data of query 2 in two graphs.

Important note: For the programs to work correctly, the file paths have to be adjusted. Scan the Comments for Asterisk lines to find these code lines.
The necessary data is freely accessible here: https://portal.electricitymaps.com/datasets
For this project hourly granularity was chosen.
