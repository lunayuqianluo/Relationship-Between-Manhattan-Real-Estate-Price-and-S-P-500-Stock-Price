# Relationship-Between-Manhattan-Real-Estate-Price-and-S-P-500-Stock-Price

#etl_code directory

HistoricalHousingPrices.csv:
To clean the original dataset of HistoricalHousingPrices.csv, I wrote HousingClean.java, HousingCleanMapper.java, and HousingCleanReducer.java. 
In the HousingCleanMapper.java, I first split the input line. Then, I only extracted saleDate, salePrice, and grossArea from every valid housing sale record.
In the HousingCleanReducer.java, I calculated the average housing price by month.
To build code, run code, and see cleaned dataset result, please follow the script on the HousingCleanScp.

HistoricalStockPrices.csv:
To clean the original dataset of HistoricalStockPrices.csv, I wrote StockClean.java, StockCleanMapper.java, and StockCleanReducer.java. 
In the StockCleanMapper.java, I first split the input line. Then, I only extracted priceDate, dayHighPrice, and dayLowPrice from every valid stock price record. Finally, I averaged the dayHighPrice and dayLowPrice then mapped priceDate to this averaged price.
In the StockCleanReducer.java, I calculated the average stock price by month.
To build code, run code, and see cleaned dataset result, please follow the script on the StockCleanScp.

Another important data cleaning point in both HistoricalHousingPrices.csv and HistoricalStockPrices.csv is to unify all housing and stock data records' date to the format of year(xx)/month(xx). This will simplify the later data visualization process.


#profiling_code directory

There are three files used for data entries counting, and they are CountRecs.java, CountRecsMapper.java, and CountRecsReducer.java.

Please follow the script on the HousingCountScpAF to get the original housing data entries' count.
Command 'make clean' to delete the previous output file.
Please follow the script on the HousingCountScpAF to get the cleaned housing data entries' count.
Command 'make clean' to delete the previous output file.

Please follow the script on the StockCountScpAF to get the original stock data entries' count.
Command 'make clean' to delete the previous output file.
Please follow the script on the StockCountScpBF to get the cleaned stock data entries' count.
Command 'make clean' to delete the previous output file.


