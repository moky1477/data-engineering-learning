## Batch Processing: 
Data Being collected from multiple sources and combined into a single source

ETL - Extract Transform Load/ ELT - Extract Load Transform:
Basically collect data from multiple data sources, perform some preprocessing steps on the raw loaded data (renaming columns/ changing units). After this, load the data into a single file which will be used for the further processes. Logging entries is also an essential step. In the end, call these functions in a specific order, which depends on the choice (ELT/ ETL)

## Web Scraping
Web scraping is used for extraction of relevant data from web pages. If you require some data from a web page in a public domain, web scraping makes the process of data extraction quite convenient. The use of web scraping, however, requires some basic knowledge of the structure of HTML pages.

## SQLite3
SQLite3 is an in-process Python library that implements a self-contained, serverless, zero-configuration, transactional SQL database engine. It is a popular choice as an embedded database for local/client storage in application software.

ETL Process: ETL stands for Extract, Transform, and Load. It involves curating data from multiple sources, transforming it into a unified format, and loading it into a new environment for analysis.

Data Extraction: Extraction is the process of obtaining or reading data from one or more sources. This can be done through methods like web scraping or using APIs to programmatically connect to data.

Data Transformation: Transformation involves processing the data to make it suitable for its destination and intended use. This includes cleaning, filtering, joining disparate data sources, feature engineering, and formatting.

Data Loading: Loading refers to writing the transformed data to a new destination environment, such as databases, data warehouses, or data marts. The goal is to make the data readily available for visualization, exploration, and further analysis.

Use Cases for ETL: ETL pipelines are used in various scenarios, such as digitizing analog data, capturing transaction history for analysis, engineering features for dashboards, and training machine learning models for prediction and decision-making.